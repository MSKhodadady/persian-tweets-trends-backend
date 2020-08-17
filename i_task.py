"""
This is an interface for managing tasks in db.
"""

import asyncio
from datetime import datetime

from i_database import DBConnection, DBRow, get_db_context
from i_program_settings import get_settings

total_crawl_task: asyncio.Task = None

async def create_task(task_type, task_state, crawl_since: datetime, crawl_until: datetime, task_user=None) -> int:
    """
    creates a task in db
    
    params:
        task_type: is one of these
            'user-crawl'
            'total-crawl'
    """

    conn: DBConnection
    async with await get_db_context() as conn:
        async with conn.transaction():

            row: DBRow = await conn.fetchrow(
                '''
                INSERT INTO public.program_task
                    (task_user, task_type, task_state, crawl_since, crawl_until)
                VALUES
                    ($1, $2, $3, $4, $5)
                RETURNING "id";
                ''',
                task_user,
                task_type,
                task_state,
                crawl_since,
                crawl_until
            )

            return row.get('id')


async def change_task_state(task_id, new_task_state):
    """
    channges the task state by its id (task_id).
    """
    conn: DBConnection
    async with await get_db_context() as conn:
        await conn.execute(
            "UPDATE public.program_task SET task_state=$1 WHERE id=$2;",
            new_task_state,
            task_id
        )
