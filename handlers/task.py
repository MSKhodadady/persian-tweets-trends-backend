from datetime import datetime

from i_database import DBConnection, DBRow, DBRows, get_db_context
from i_main_handler import MainHandler


class TaskHandler(MainHandler):

    async def get(self):

        start = int(self.get_query_argument('start', 0))
        count = int(self.get_query_argument('count', 10))

        conn: DBConnection
        async with await get_db_context() as conn:
            async with conn.transaction():
                rows: DBRows = await conn.fetch(
                    """
                    SELECT 
                        id, task_user, task_type, task_state, created_at, crawl_since, crawl_until
                    FROM public.program_task
                    order by crawl_until desc
                    limit $1 offset $2;
                    """,
                    count, start
                )

                def row_to_json(row: DBRow):
                    row_dict = dict(row)

                    for k in row_dict:
                        if isinstance(row_dict[k], datetime):
                            d: datetime = row_dict[k]
                            row_dict[k] = d.isoformat()

                    return row_dict

                self.write({
                    "data": list(
                        map(row_to_json, rows)
                    )
                })
