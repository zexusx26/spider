import time
import tracemalloc
from argparse import ArgumentParser
from asyncio import get_event_loop

from aiohttp import ClientSession
from humanfriendly import format_size, format_timespan

from db import DB
from scrapper import Scrapper

USER = 'scrapper'
PASSWORD = 'password'
DATABASE = 'scrapping'


async def load(url: str, depth: int = 0):
    tracemalloc.start()
    now = time.time()
    async with ClientSession() as session, DB(USER, PASSWORD, DATABASE) as db:
        scrapper = Scrapper(url, session, db)
        await scrapper.scrape(scrapper.doctor(url), depth=depth)
        await scrapper.flush()
        scrapper.clear_message()
    exec_time = format_timespan(time.time() - now)
    peak_mem = format_size(tracemalloc.get_traced_memory()[1])
    print(f'ok, execution time: {exec_time}, peak memory usage: {peak_mem}')


async def truncate():
    tracemalloc.start()
    now = time.time()
    async with DB(USER, PASSWORD, DATABASE) as db:
        await db.truncate()
    exec_time = format_timespan(time.time() - now)
    peak_mem = format_size(tracemalloc.get_traced_memory()[1])
    print(f'ok, execution time: {exec_time}, peak memory usage: {peak_mem}')


async def get(url: str, counter: int = 1):
    tracemalloc.start()
    now = time.time()
    base_domain = Scrapper.get_base_domain(url)
    async with DB(USER, PASSWORD, DATABASE) as db:
        async for record in db.get_URLs(base_domain, counter):
            print(f'{record["url"]}: "{record["title"]}"')
    exec_time = format_timespan(time.time() - now)
    peak_mem = format_size(tracemalloc.get_traced_memory()[1])
    print(f'ok, execution time: {exec_time}, peak memory usage: {peak_mem}')


COMMANDS = {
    'load': lambda args: load(args.url, args.depth),
    'get': lambda args: get(args.url, args.n),
    'truncate': lambda args: truncate()
}


DESCRIPTION = """Python developer test task.
Commands:
"load": load URLs, titles and HTML from web;
"get": get URLs and titles from database;
"truncate": delete all data from database.
"""


if __name__ == '__main__':

    parser = ArgumentParser(description=DESCRIPTION)
    parser.add_argument('command', choices=COMMANDS, help='command')
    parser.add_argument('url', nargs='?', help='URL (required for commands "load" and "get")')
    parser.add_argument('--depth', type=int, help='scrapping depth (required for command "load")', default=0)
    parser.add_argument('-n', type=int, help='records quantity (required for command "get")', default=1)
    args = parser.parse_args()

    task = COMMANDS[args.command](args)

    loop = get_event_loop()
    loop.run_until_complete(task)
