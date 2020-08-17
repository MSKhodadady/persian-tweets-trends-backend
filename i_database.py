"""
This module is used for providing db connection
"""

from typing import List

import asyncpg

from project_secrets import postgres_secret

#: types
#: provides some types for referencing
DBConnection = asyncpg.Connection
DBRow = asyncpg.Record
DBRows = List[DBRow]

#: connection pool object
__db_connection_pool: asyncpg.pool.Pool = None


async def get_db_context() -> asyncpg.pool.PoolAcquireContext:
    """
    returns one connection from the connection pool
    """

    if __db_connection_pool is None:
        await __init_db()

    return __db_connection_pool.acquire()


async def __init_db():
    """
    initiates db connection pool
    """

    global __db_connection_pool

    __db_connection_pool = await asyncpg.create_pool(
        database=postgres_secret['database'],
        user=postgres_secret['user'],
        password=postgres_secret['password']
    )
