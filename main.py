"""
Program entry point
"""

import asyncio
import os
from datetime import datetime, timedelta

import socketio
import tornado
from tornado.web import url as rout_url

import handlers
import i_socket_io
from crawl_analyze.crawl import crawl_total
from i_program_settings import get_settings
from i_task import change_task_state, create_task


def prepare_tornado():
    """
    specifies routes, port and ...
    """

    #: program routes and options
    app = tornado.web.Application([
        rout_url(
            r"/socket",
            socketio.get_tornado_handler(i_socket_io.get_sio())
        ),
        
        rout_url(r"/api/user", handlers.user.UserHandler),
        rout_url(r"/api/dict", handlers.dictionary.DictionaryHandler),
        rout_url(r"/api/task", handlers.task.TaskHandler),
        rout_url(r"/api/chart", handlers.chart.ChartHandler),
        # rout_url(r"/api/chart/search", handlers.chart.ChartSearchHandler),

        rout_url(
            r"/(.*)", tornado.web.StaticFileHandler,
            {'default_filename': 'index.html', 'path': os.path.dirname(__file__) + '/frontend/'}
        ),
    ])

    port = 5000
    app.listen(port)
    print(f"app started on port: {port}")


async def total_crawl_runner():
    """
    Crawles users with crawl peremission ('isCrawl' column must be true for the user in db),
    in time intervals.
    After each crawl, sleeps for a while, specified by 'crawl-interval-hour' in program settings.
    """

    #: intital crawl period
    until = datetime.now()
    since = until - timedelta(hours=get_settings('crawl-interval-hour'))

    while True:
        try:
            #: 'since' for next intrval
            new_since = datetime.now()

            #: sleep
            sleep_count_hour = get_settings('crawl-interval-hour')

            print(f'[total-crawl][sleep for {sleep_count_hour} hours]')
            
            await asyncio.sleep(
                sleep_count_hour * 60 * 60
            )

            #: create crawl task in db
            task_id = await create_task(
                'total-crawl', 'not started', since, until
            )

            #: crawl
            print(f'[total-crawl][task:{task_id}][starting crawl][from {since.isoformat()} to {until.isoformat()}]')
            all_user_count, all_tweet_crawled_count = await crawl_total(
                task_id,
                since.strftime("%Y-%m-%d %H:%M:%S"),
                until.strftime("%Y-%m-%d %H:%M:%S")
            )

            message = f'crawled:{all_tweet_crawled_count} tweets of {all_user_count} user'
            print(f'[total-crawl][task:{task_id}][end crawl][{message}]')

            await change_task_state(task_id, message)

            #: set the period for next interval
            since = new_since
            until = datetime.now()

        except Exception as e:
            #: because of error, since will not change
            until = datetime.now()
            print(f'[total-crawl][task:{task_id}][error][{e}]')

if __name__ == "__main__":
    prepare_tornado()

    loop = asyncio.get_event_loop()
    loop.create_task(total_crawl_runner())

    tornado.ioloop.IOLoop.current().start()
