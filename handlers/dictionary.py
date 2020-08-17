"""
Handler for dictionary api
"""
import asyncpg

from i_database import DBConnection, get_db_context
from i_main_handler import MainHandler


class DictionaryHandler(MainHandler):

    async def get(self):
        """
        gets some dictionaries

        query arguments: used for specify returning page
            start
            count
        """
        start = int(self.get_query_argument('start', 0))
        count = int(self.get_query_argument('count', 10))

        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():

                rows = await conn.fetch(
                    'SELECT "token", replace_with as "replaceWith" FROM public."dictionary" limit $1 offset $2;', count, start)

                self.write({
                    "data": list(
                        map(dict, rows)
                    )
                })

    async def put(self):
        """
        creates a record in 'dictionary' table.
        also check for all tokens in 'tweet_token' table for this token.

        input:
        {
            "token": str,
            "replace-with": nullable str
        }

        returns:
        {
            "token"
        }
        """
        token = self.get_json_arg('token')
        replace_with = self.get_json_arg('replace-with', None)

        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():

                #: insert it in 'dictionary' table
                token_db = await conn.fetchrow(
                    'insert into public."dictionary" ("token", replace_with) values($1, $2) returning "token";',
                    token, replace_with
                )

        dupError = False
        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():
                if replace_with:
                    #: replace old tokens with 'replace_with'
                    try:
                        await conn.execute(
                            'update "tweet_token" set "token" = $1 where "token" = $2;',
                            replace_with, token
                        )
                    #: if another token exists in db that is same as replace_with
                    except asyncpg.exceptions.UniqueViolationError:
                        dupError = True
                else:
                    #: delete tokens if 'replace_with' is null
                    await conn.execute(
                        'DELETE FROM public.tweet_token WHERE "token"=$1;',
                        token
                    )
        
        if dupError:
            async with await get_db_context() as conn:
                async with conn.transaction():
                    await conn.execute(
                        'DELETE FROM public.tweet_token WHERE "token"=$1;',
                        token
                    )

        self.write({
            'token': token_db.get('token')
        })

    async def delete(self):
        """
        deletes 'dictionary' record with specified 'token'
        """
        token = self.get_json_arg('token')

        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():

                await conn.execute(
                    'DELETE FROM public."dictionary" WHERE "token"=$1;',
                    token
                )

        self.write({
            "token": token
        })
