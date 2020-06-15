import asyncio
from typing import Iterable, Tuple

from asyncpg import Record

from spider.db import DB

from .fixtures import async_test, event_loop

USER = 'spider'
PASSWORD = 'friendlyneighborhoodspider'
DATABASE = 'spiderdata'
HOST = 'db'


#################################
# ИНИЦИАЛИЗАТОРЫ И ФИНАЛИЗАТОРЫ #
#################################

async def async_setup_module():
    query = 'CREATE SCHEMA IF NOT EXISTS spider'
    async with DB(USER, PASSWORD, DATABASE, HOST) as db:
        await db.execute(query)
    query = 'CREATE TABLE IF NOT EXISTS scrapped_data (url TEXT PRIMARY KEY, title TEXT, html TEXT)'
    async with DB(USER, PASSWORD, DATABASE, HOST) as db:
        await db.execute(query)


def setup_module(module):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_setup_module())
    loop.close()


async def async_teardown_module():
    queries = [
        'DROP TABLE scrapped_data',
        'DROP SCHEMA spider'
    ]
    async with DB(USER, PASSWORD, DATABASE, HOST) as db:
        for query in queries:
            return await db.execute(query)


###########
# УТИЛИТЫ #
###########

def teardown_module(module):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_teardown_module())
    loop.close()


def record2tuple(record: Record) -> Tuple[str, str]:
    return record['url'], record['title']


def compare_records(record: Tuple[str, str, str], test_record: Record):
    for i, name in enumerate(('url', 'title')):
        assert record[i] == test_record[name]


def compare_records_sets(records: Iterable[Tuple[str, str, str]], test_records: Iterable[Record]):

    records = {record[0:2] for record in records}
    test_records = {(test_record['url'], test_record['title']) for test_record in test_records}
    assert records == test_records


async def truncate_table(db: DB):

    query = 'TRUNCATE TABLE scrapped_data'
    await db.execute(query)


##############################
# АСИНХРОННЫЕ ФУНКЦИИ ТЕСТОВ #
##############################

@async_test
async def test_add_record(event_loop):

    record = ('https://example.com', 'title', 'html')

    async with DB(USER, PASSWORD, DATABASE, HOST) as db:

        await db.add_records([record])

        counter = 0
        async for test_record in db.get_records('example.com'):
            counter += 1
            compare_records(record, test_record)
        assert counter == 1

        counter = 0
        async for test_record in db.get_records('another.example.com'):
            counter += 1
        assert counter == 0

        counter = 0
        async for test_record in db.get_records('another_example.com'):
            counter += 1
        assert counter == 0

        await truncate_table(db)


@async_test
async def test_add_records(event_loop):

    records = [
        ('https://example.com', 'title', 'html'),
        ('https://another.example.com', 'another_title', 'another_html')
    ]

    async with DB(USER, PASSWORD, DATABASE, HOST) as db:

        await db.add_records(records)

        test_records = [test_record async for test_record in db.get_records('example.com')]
        assert len(test_records) == 2
        compare_records_sets(records, test_records)

        test_records = [test_record async for test_record in db.get_records('another.example.com')]
        assert len(test_records) == 1
        compare_records(records[1], test_records[0])

        await truncate_table(db)


@async_test
async def test_get_records(event_loop):

    records = [(f'https://{i}.example.com', f'title{i}', f'html{i}') for i in range(10)]

    async with DB(USER, PASSWORD, DATABASE, HOST) as db:

        await db.add_records(records)

        records = {record[0:2] for record in records}

        test_records = [test_record async for test_record in db.get_records('example.com', 1)]
        assert len(test_records) == 1
        test_record = record2tuple(test_records[0])
        assert test_record in records

        test_records = [test_record async for test_record in db.get_records('example.com', 2)]
        assert len(test_records) == 2
        test_records = {record2tuple(test_record) for test_record in test_records}
        assert len(test_records - records) == 0

        test_records = [test_record async for test_record in db.get_records('example.com', 5)]
        assert len(test_records) == 5
        test_records = {record2tuple(test_record) for test_record in test_records}
        assert len(test_records - records) == 0

        test_records = [test_record async for test_record in db.get_records('example.com', 20)]
        assert len(test_records) == 10
        test_records = {record2tuple(test_record) for test_record in test_records}
        assert len(test_records - records) == 0

        await truncate_table(db)


@async_test
async def test_get_many_records(event_loop):

    records = [(f'https://{i}.example.com', f'title{i}', f'html{i}') for i in range(10)]
    records += [(f'https://{i}.another.example.com', f'another_title{i}', f'another_html{i}') for i in range(10)]

    async with DB(USER, PASSWORD, DATABASE, HOST) as db:

        await db.add_records(records)

        records = {record[0:2] for record in records}

        test_records = [test_record async for test_record in db.get_records('example.com', 1)]
        assert len(test_records) == 1
        test_record = record2tuple(test_records[0])
        assert test_record in records

        test_records = [test_record async for test_record in db.get_records('example.com', 3)]
        assert len(test_records) == 3
        test_records = {record2tuple(test_record) for test_record in test_records}
        assert len(test_records - records) == 0

        test_records = [test_record async for test_record in db.get_records('example.com', 7)]
        assert len(test_records) == 7
        test_records = {record2tuple(test_record) for test_record in test_records}
        assert len(test_records - records) == 0

        test_records = [test_record async for test_record in db.get_records('example.com', 15)]
        assert len(test_records) == 15
        test_records = {record2tuple(test_record) for test_record in test_records}
        assert len(test_records - records) == 0

        test_records = [test_record async for test_record in db.get_records('another.example.com', 1)]
        assert len(test_records) == 1
        test_record = record2tuple(test_records[0])
        assert test_record in records

        test_records = [test_record async for test_record in db.get_records('another.example.com', 3)]
        assert len(test_records) == 3
        test_records = {record2tuple(test_record) for test_record in test_records}
        assert len(test_records - records) == 0

        test_records = [test_record async for test_record in db.get_records('another.example.com', 7)]
        assert len(test_records) == 7
        test_records = {record2tuple(test_record) for test_record in test_records}
        assert len(test_records - records) == 0

        test_records = [test_record async for test_record in db.get_records('another.example.com', 15)]
        assert len(test_records) == 10
        test_records = {record2tuple(test_record) for test_record in test_records}
        assert len(test_records - records) == 0

        await truncate_table(db)
