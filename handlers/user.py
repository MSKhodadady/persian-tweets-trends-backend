import asyncio
from datetime import datetime
from typing import List

from crawl_analyze.crawl import crawl_user
from i_database import DBConnection, DBRow, get_db_context
from i_main_handler import MainHandler
from i_task import create_task


class UserHandler(MainHandler):

    async def get(self):

        count = int(self.get_query_argument('count', 10))
        start = int(self.get_query_argument('start', 0))

        username = self.get_query_argument('username', None)

        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():

                if username:
                    firs_last = await conn.fetchrow(
                        """
                        select min(t.tweet_time) as "from", max(t.tweet_time) as "to" from tweet t
                        inner join twitter_user on t.username = twitter_user.username
                        where t.username = $1;
                        """,
                        username
                    )

                    firs_last_d = {
                        "from": str(firs_last.get('to')),
                        "to": str(firs_last.get('to'))
                    }

                    count = await conn.fetchrow(
                        "select count(t.tweet_id) from tweet t where username = $1;",
                        username
                    )

                    self.write({
                        "username": username,
                        **dict(firs_last_d),
                        **dict(count)
                    })

                else:
                    rows: List[DBRow] = await conn.fetch(
                        "select username, iscrawl from twitter_user limit $1 offset $2", count, start)

                    self.write({
                        "data": list(
                            map(dict, rows)
                        )
                    })

    async def put(self):

        username = self.get_json_arg('username')

        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO public.twitter_user (username, iscrawl, created_at) VALUES($1, true, $2);",
                    username, datetime.now()
                )

                self.write({
                    "username": username
                })

    async def delete(self):

        username = self.get_json_arg('username')

        delete_tweets = self.get_json_arg('delete-tweets', None)
        if delete_tweets:
            tweet_delete_start = datetime.fromisoformat(delete_tweets['start'])
            tweet_delete_end = datetime.fromisoformat(delete_tweets['end'])

        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():
                if delete_tweets:
                    await conn.execute(
                        """
                        delete from public.tweet t
                        where
                            t.tweet_time >= $1 and
                            t.tweet_time <= $2 and
                            t.username = $3;
                        """,
                        tweet_delete_start,
                        tweet_delete_end,
                        username
                    )
                else:
                    await conn.execute(
                        "DELETE FROM public.twitter_user WHERE username=$1;",
                        username
                    )

                self.write({
                    "username": username
                })

    async def patch(self):

        username = self.get_json_arg('username')
        is_crawl: bool = self.get_json_arg('is-crawl')

        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():
                await conn.execute(
                    "update public.twitter_user set iscrawl= $1 WHERE username= $2;",
                    is_crawl, username
                )

                self.write({
                    "username": username
                })

    async def post(self):
        """
        This method is used for crawling a user
        """

        username = self.get_json_arg('username')
        crawl_since = self.get_json_arg('crawl-since')
        crawl_until = self.get_json_arg('crawl-until')

        #: create crawl task in db
        task_id = await create_task(
            'user-crawl',
            'not-crawled',
            datetime.fromisoformat(crawl_since),
            datetime.fromisoformat(crawl_until),
            username
        )

        #: send the task id to client
        self.write({
            "task_id": task_id
        })


        # : create a 'async task' (not or task) for crawling user
        asyncio.create_task(
            crawl_user(
                username,
                task_id,
                crawl_since,
                crawl_until
            )
        )
