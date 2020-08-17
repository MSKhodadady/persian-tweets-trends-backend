import json
import os
from datetime import datetime
from functools import reduce as fold_l

import asyncpg
import twint
from twint import Config

from i_database import DBConnection, DBRow, get_db_context
from i_program_settings import get_all_settings
from i_socket_io import get_sio
from i_task import change_task_state

from .text_analyzer import text_analyze


async def crawl_user(
    username, task_id, since: str, until: str, user_crawl: bool = True) -> int:

    """
    This function, crawls a user

    Params
        username
        task_id: the id of task that crawles the tweets
        since: crawl from
        until: crawl until
        user_crawl:
            True -> crawler task is 'user-crawl' task
            False -> crawl task is 'total-crawl' task or another

    returns:
        count of tweets crawled
    """

    #: json output file for twint
    output_file = \
        os.path.dirname(__file__) + \
        f"/../outputfiles/tweets/{username}.{int(datetime.now().timestamp())}.json"

    #: create config of crawl
    conf = get_crawl_config(
        username,
        since,
        until,
        output_file
    )

    if user_crawl:
        print(f'[user-crawl][task:{task_id}][start crawling]')

        #: send state to socket
        await get_sio().emit('task', {
            'id': task_id,
            'state': "crawling"
        })

    #: crawl with twint
    await twint.run.Twint(conf).main()

    try:
        async def tweet_load_callback(row_num, all_count):
            if user_crawl:
                await get_sio().emit('task', {
                    'id': task_id,
                    'state': f'analyze$save tweet {row_num} of {all_count}',
                    'percent': row_num * 100 / all_count
                })

        #: extract tweets from twint output file
        all_count = await extract_tweets(
            output_file, 
            task_id, 
            tweet_load_callback
        )
    except FileNotFoundError:
        all_count = 0

    if user_crawl:
        all_count_message = 'NO-TWEET' if all_count == 0 else all_count

        print(f'[user-crawl][task:{task_id}][analyze&save tweet end, {all_count_message} crawled]')
        #: emit crawl end message
        await get_sio().emit('task', {
            'id': task_id,
            'state': f'analyze&save tweet end, {all_count_message} crawled',
        })

        #: change state of task
        await change_task_state(task_id, f'{all_count} tweet crawled')

    return all_count


async def extract_tweets(output_file, task_id, tweet_load_callback_async = None) -> int:

    #: read count of 
    with open(output_file) as o: 
        all_count = fold_l(
            lambda acc, x: acc + 1, 
            o, 0
        )

    row_num = 0

    with open(output_file, 'r') as json_file:
        #: every line of output is a tweet
        for line in json_file:

            row_num +=1
            if tweet_load_callback_async: await tweet_load_callback_async(row_num, all_count)

            #: load it
            tweet: dict = json.loads(line)

            try:
                #: save tweet in db
                await tweet_save_db(tweet, task_id)

                #: analyze tweet and save tokens in db
                await analyze_save_db(
                    await text_analyze(tweet['tweet']),
                    tweet['id']
                )

            #: don't attention to duplicates
            except asyncpg.exceptions.UniqueViolationError as e:
                pass
            except Exception as e:
                raise e

    return all_count


async def tweet_save_db(tweet_dict: dict, task_id):
    """
    saves a tweet in database

    task_id: it is used for show the tweet is for which crawling task
    """
    conn: DBConnection
    async with await get_db_context() as conn:
        async with conn.transaction():

            tweet_time = f'{tweet_dict["date"]} {tweet_dict["time"]}{tweet_dict["timezone"]}'
            await conn.execute(
                f"""
                INSERT INTO public.tweet
                (tweet_id, tweet_text, username, crawler_task, tweet_time)
                VALUES(
                    $1, $2, $3, $4,
                    '{tweet_time}');
                """,
                tweet_dict['id'],
                tweet_dict['tweet'],
                tweet_dict['username'],
                task_id
            )


async def analyze_save_db(analyze_list: list, tweet_id):
    """
    This function saves the list of tokens is db.
    It also checks the 'dictionary' finding that a token must be replaced or deleted.
    """

    for token in analyze_list:
        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():

                #: check the 'dictionary' for token
                row: DBRow = await conn.fetchrow(
                    'SELECT replace_with FROM public."dictionary" where "token" = $1;', token
                )

                #: if exists in 'dictionary'
                if row:
                    replace_with = row.get('replace_with')

                    #: if it must be replaced with another token
                    if replace_with:
                        await conn.execute(
                            'INSERT INTO public.tweet_token (tweet_id, "token") VALUES($1, $2);',
                            tweet_id, replace_with
                        )
                    
                    #: or if it shouldn't be inserted in db
                    else:
                        continue
                else:
                    await conn.execute(
                        'INSERT INTO public.tweet_token (tweet_id, "token") VALUES($1, $2);',
                        tweet_id, token
                    )


async def crawl_total(task_id, since: str, until: str) -> tuple:
    """
    This function:
        1- checks for user that must be crawled
        2- crawl them one by one

    returns:
        - count of all tweets crawled
        - count of all users crawled
    """

    #: open db connection
    conn: DBConnection
    async with await get_db_context() as conn:
        async with conn.transaction():

            #: read db all users that can be crawled
            all_user_count = (
                await conn.fetchrow('select count("username") from "twitter_user" where "iscrawl";')
            ).get('count')

            user_num = 0

            all_tweet_crawled_count = 0

            row: DBRow
            async for row in conn.cursor('select "username" from "twitter_user" where "iscrawl";'):
                user_num += 1

                #: emit state to client
                await get_sio().emit('task', {
                    "id": task_id,
                    "state": f"crawling user: {row.get('username')}",
                    "percent": user_num * 100 / all_user_count
                })

                #: crawl user
                crawl_count = await crawl_user(
                    row.get('username'),
                    task_id,
                    since,
                    until,
                    user_crawl=False
                )

                all_tweet_crawled_count += crawl_count

    return all_user_count, all_tweet_crawled_count


def get_crawl_config(username, since: str, until: str, output_file):
    c = Config()

    #: copied from: twint lib -> run.py -> Search function
    c.TwitterSearch = True
    c.Favorites = False
    c.Following = False
    c.Followers = False
    c.Profile = False
    c.Profile_full = False

    c.Output = output_file
    c.Username = username
    c.Store_json = True
    c.Format = "{username} | id: {id}"
    c.Until = until
    c.Since = since
    c.Filter_retweets = True

    #: configs from program_settings
    setting = get_all_settings()
    if setting["use-proxy"]:
        c.Proxy_host = setting["proxy-host"]
        c.Proxy_port = setting["proxy-port"]
        c.Proxy_type = setting["proxy-type"]

    c.Hide_output = not setting["show-twint-output"]

    return c
