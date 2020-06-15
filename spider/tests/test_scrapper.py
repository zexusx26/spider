import asyncio
import os
import sys
from typing import Iterable, Tuple

import pytest

from spider.scrapper import Scrapper

from .fixtures import event_loop
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


##############################
# АСИНХРОННЫЕ ФУНКЦИИ ТЕСТОВ #
##############################

async def async_test_scrape_empty():

    url = 'https://example.com'
    heads = {
        url: {
            'Content-Type': 'text/html'
        }
    }
    contents = {
        url: load_page('empty')
    }

    session_mock = SessionMock(heads, contents)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url, depth=0)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'done' in scrapper.stat
    assert scrapper.stat['done'] == 1

    assert len(db_mock.records) == 1
    record = db_mock.records[0]
    assert record[0] == url
    assert record[1] == 'Document'
    assert record[2] == contents[url]


async def async_test_scrape_wrong_header():

    url = 'https://example.com'
    heads = {
        url: {
            'Content-Type': 'application/json'
        }
    }

    session_mock = SessionMock(heads)
    db_mock = DBMock()

    scrapper = Scrapper(url, session_mock, db_mock)
    await scrapper.scrape(url, depth=5)
    await scrapper.flush()

    assert len(scrapper.stat) == 1
    assert 'wrong_content_type' in scrapper.stat
    assert 'application/json' in scrapper.stat['wrong_content_type']
    assert scrapper.stat['wrong_content_type']['application/json'] == 1

    assert len(db_mock.records) == 0


#############################
# СИНХРОННЫЕ ФУНКЦИИ ТЕСТОВ #
#############################

def test_scrape_empty(event_loop: asyncio.AbstractEventLoop):
    event_loop.run_until_complete(async_test_scrape_empty())

def test_scrape_wrong_header(event_loop: asyncio.AbstractEventLoop):
    event_loop.run_until_complete(async_test_scrape_wrong_header())
