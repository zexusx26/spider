import asyncio
import os

from aiohttp import ClientError

from spider.scrapper import Scrapper

from .fixtures import async_test, event_loop
from .mocks import DBMock, SessionMock

###########
# УТИЛИТЫ #
###########


def load_page(page_name: str) -> str:
    page_name += '.html'
    base = os.path.join(os.path.split(__file__)[0], 'pages')
    fp = os.path.join(base, page_name)
    with open(fp) as file:
        return file.read()


def timeout_raser():
    raise asyncio.TimeoutError


def client_error_raiser():
    raise ClientError


def unicode_decode_error_raiser():
    raise UnicodeDecodeError('spidercodec', b'\x00\x00', 1, 2, 'This is just a fake reason!')


##############################
# АСИНХРОННЫЕ ФУНКЦИИ ТЕСТОВ #
##############################

@async_test
async def test_simple_scrape(event_loop):

    url = 'https://example.com/0'
    urls = {
        url: {
            'head_value': {'Content-Type': 'text/html'},
            'text_value': load_page('0')
        }
    }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'done' in scrapper.stat
    assert scrapper.stat['done'] == 1

    assert len(db_mock.records) == 1
    record = db_mock.records[0]
    assert record[0] == url
    assert record[1] == '0'
    assert record[2] == urls[url]['text_value']


@async_test
async def test_simple_scrape_with_extended_head(event_loop):

    url = 'https://example.com/0'
    urls = {
        url: {
            'head_value': {'Content-Type': 'text/html;charset=utf-8'},
            'text_value': load_page('0')
        }
    }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'done' in scrapper.stat
    assert scrapper.stat['done'] == 1

    assert len(db_mock.records) == 1
    record = db_mock.records[0]
    assert record[0] == url
    assert record[1] == '0'
    assert record[2] == urls[url]['text_value']


@async_test
async def test_simple_scrape_with_wrong_head(event_loop):

    url = 'https://example.com/0'
    urls = {
        url: {
            'head_value': {'Content-Type': 'application/json'}
        }
    }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'wrong_content_type' in scrapper.stat
    assert 'application/json' in scrapper.stat['wrong_content_type']
    assert scrapper.stat['wrong_content_type']['application/json'] == 1

    assert len(db_mock.records) == 0


@async_test
async def test_simple_scrape_with_head_timeout(event_loop):

    url = 'https://example.com/0'
    urls = {
        url: {
            'head_action': timeout_raser
        }
    }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    scrapper.SLEEP_TIME = 0.1
    await scrapper.scrape(url)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'connection_error' in scrapper.stat
    assert scrapper.stat['connection_error'] == 1

    assert len(db_mock.records) == 0


@async_test
async def test_simple_scrape_with_head_error(event_loop):

    url = 'https://example.com/0'
    urls = {
        url: {
            'head_action': client_error_raiser
        }
    }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    scrapper.SLEEP_TIME = 0.1
    await scrapper.scrape(url)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'connection_error' in scrapper.stat
    assert scrapper.stat['connection_error'] == 1

    assert len(db_mock.records) == 0


@async_test
async def test_simple_scrape_with_get_timeout(event_loop):

    url = 'https://example.com/0'
    urls = {
        url: {
            'head_value': {'Content-Type': 'text/html'},
            'get_action': timeout_raser
        }
    }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    scrapper.SLEEP_TIME = 0.1
    await scrapper.scrape(url)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'connection_error' in scrapper.stat
    assert scrapper.stat['connection_error'] == 1

    assert len(db_mock.records) == 0


@async_test
async def test_simple_scrape_with_get_error(event_loop):

    url = 'https://example.com/0'
    urls = {
        url: {
            'head_value': {'Content-Type': 'text/html'},
            'get_action': client_error_raiser
        }
    }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    scrapper.SLEEP_TIME = 0.1
    await scrapper.scrape(url)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'connection_error' in scrapper.stat
    assert scrapper.stat['connection_error'] == 1

    assert len(db_mock.records) == 0


@async_test
async def test_simple_scrape_with_text_error(event_loop):

    url = 'https://example.com/0'
    urls = {
        url: {
            'head_value': {'Content-Type': 'text/html'},
            'text_action': unicode_decode_error_raiser
        }
    }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    scrapper.SLEEP_TIME = 0.1
    await scrapper.scrape(url)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'unicode_decode_error' in scrapper.stat
    assert scrapper.stat['unicode_decode_error'] == 1

    assert len(db_mock.records) == 0


@async_test
async def test_0_depth(event_loop):

    url = 'https://example.com/1'
    urls = {
        url: {
            'head_value': {'Content-Type': 'text/html'},
            'text_value': load_page('1')
        }
    }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url, 0)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'done' in scrapper.stat
    assert scrapper.stat['done'] == 1

    assert len(db_mock.records) == 1
    record = db_mock.records[0]
    assert record[0] == url
    assert record[1] == '1'
    assert record[2] == urls[url]['text_value']


@async_test
async def test_1_depth(event_loop):

    load_deep = 2
    scrapped_deep = 1

    urls = {}
    for i in range(load_deep):
        url = f'https://example.com/{i}'
        urls[url] = {
            'head_value': {'Content-Type': 'text/html'},
            'text_value': load_page(str(i))
        }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url, scrapped_deep)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'done' in scrapper.stat
    assert scrapper.stat['done'] == scrapped_deep + 1

    assert len(db_mock.records) == scrapped_deep + 1

    for record in db_mock.records:
        url = record[0]
        assert url in urls
        assert record[1] == url[-1]
        assert record[2] == urls[url]['text_value']
        urls.pop(url)


@async_test
async def test_1_depth_one_more_time(event_loop):

    load_deep = 3
    scrapped_deep = 1

    urls = {}
    for i in range(load_deep):
        url = f'https://example.com/{i}'
        urls[url] = {
            'head_value': {'Content-Type': 'text/html'},
            'text_value': load_page(str(i))
        }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url, scrapped_deep)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'done' in scrapper.stat
    assert scrapper.stat['done'] == scrapped_deep + 1

    assert len(db_mock.records) == scrapped_deep + 1

    for record in db_mock.records:
        url = record[0]
        assert url in urls
        assert record[1] == url[-1]
        assert record[2] == urls[url]['text_value']
        urls.pop(url)


@async_test
async def test_2_depth(event_loop):

    load_deep = 3
    scrapped_deep = 2

    urls = {}
    for i in range(load_deep):
        url = f'https://example.com/{i}'
        urls[url] = {
            'head_value': {'Content-Type': 'text/html'},
            'text_value': load_page(str(i))
        }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url, scrapped_deep)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'done' in scrapper.stat
    assert scrapper.stat['done'] == scrapped_deep + 1

    assert len(db_mock.records) == scrapped_deep + 1

    for record in db_mock.records:
        url = record[0]
        assert url in urls
        assert record[1] == url[-1]
        assert record[2] == urls[url]['text_value']
        urls.pop(url)


@async_test
async def test_3_depth(event_loop):

    load_deep = 3
    scrapped_deep = 3

    urls = {}
    for i in range(load_deep):
        url = f'https://example.com/{i}'
        urls[url] = {
            'head_value': {'Content-Type': 'text/html'},
            'text_value': load_page(str(i))
        }

    session_mock = SessionMock(urls)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url, scrapped_deep)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'done' in scrapper.stat
    assert scrapper.stat['done'] == scrapped_deep

    assert len(db_mock.records) == scrapped_deep

    for record in db_mock.records:
        url = record[0]
        assert url in urls
        assert record[1] == url[-1]
        assert record[2] == urls[url]['text_value']
        urls.pop(url)
