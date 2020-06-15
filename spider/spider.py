#!/bin/python3
import time
import tracemalloc
from argparse import ArgumentParser
from asyncio import get_event_loop
from typing import Callable

from aiohttp import ClientSession
from humanfriendly import format_size, format_timespan

from db import DB
from scrapper import Scrapper

USER = 'spider'
PASSWORD = 'friendlyneighborhoodspider'
DATABASE = 'spiderdata'
HOST = 'db'


def async_profiler(func) -> Callable:
    """ Профайлер для асинхронных функций.

    Замеряет время выполнения и пик потребления памяти.

    :param func: асинхронная функция
    :type func: Callable
    """
    async def wrapper(*args, **kwargs):
        tracemalloc.start()
        now = time.time()
        res = await func(*args, **kwargs)
        exec_time = format_timespan(time.time() - now)
        peak_mem = format_size(tracemalloc.get_traced_memory()[1])
        print(f'ok, execution time: {exec_time}, peak memory usage: {peak_mem}')
        return res
    return wrapper


@async_profiler
async def load(url: str, depth: int = 0):
    """ Обойти сайт и сохранить html, URL и заголовок в БД.

    :param url: URL начала обхода
    :type url: str
    :param depth: глубина обхода, defaults to 0
    :type depth: int, optional
    """
    async with ClientSession() as session, DB(USER, PASSWORD, DATABASE, HOST) as db:
        scrapper = Scrapper(url, session, db)
        await scrapper.scrape(scrapper.doctor(url), depth=depth)
        await scrapper.flush()
        scrapper.clear_message()


async def get(url: str, counter: int = 1):
    """ Получить URL и заголовки загруженных страниц.

    Записи будут отфильтрованы: из переданного URL будет получен домен второго уровня. Для URL всех записей этот домен
    должен быть подстрокой.

    :param url: URL
    :type url: str
    :param counter: число требуемых записей, defaults to 1
    :type counter: int, optional
    """
    base_domain = Scrapper.get_base_domain(url)
    async with DB(USER, PASSWORD, DATABASE, HOST) as db:
        async for record in db.get_records(base_domain, counter):
            print(f'{record["url"]} -> "{record["title"]}"')


COMMANDS = {
    'load': lambda args: load(args.url, args.depth),
    'get': lambda args: get(args.url, args.n)
}


DESCRIPTION = """Python developer test task.
Commands:
"load": load URLs, titles and HTML from web;
"get": get URLs and titles from database.
"""


if __name__ == '__main__':

    # парсим аргументы
    parser = ArgumentParser(description=DESCRIPTION)
    parser.add_argument('command', choices=COMMANDS, help='command')
    parser.add_argument('url', help='URL')
    parser.add_argument('--depth', type=int, help='scrapping depth (required for command "load")', default=0)
    parser.add_argument('-n', type=int, help='records quantity (required for command "get")', default=1)
    args = parser.parse_args()

    # определяем задачу
    task = COMMANDS[args.command](args)

    # выполняем задачу
    loop = get_event_loop()
    loop.run_until_complete(task)
