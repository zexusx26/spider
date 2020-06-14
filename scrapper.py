import sys
from asyncio import Task, gather, sleep
from typing import List, Tuple, Union
from urllib.parse import urljoin, urlparse

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientOSError, ServerConnectionError
from bs4 import BeautifulSoup

from db import DB


class Scrapper:

    MAX_ATTEMPTS = 5
    FLUSH_SIZE = 100

    _base_domain: str
    _scheme: str
    _scrapped_urls: set
    _known_urls: set
    _session: ClientSession
    _db: DB
    _data: List[Tuple[str, str, str]]
    _total: int = 1
    _done: int = 0
    _message: str = ''

    @staticmethod
    def doctor(url: str) -> str:
        if '#' in url:
            url = url[:url.find('#')]
        url = url.strip('/')
        return url

    @staticmethod
    def get_base_domain(url: str) -> str:
        url = urlparse(url)
        netloc = url.netloc
        return netloc.split('.', netloc.count('.') - 1)[-1]

    def rel2abs(self, path: str, base: str) -> str:
        base = urlparse(base)
        base = '://'.join((base.scheme, base.netloc))
        return urljoin(base, path)

    @staticmethod
    def get_scheme(url: str) -> str:
        url = urlparse(url)
        return url.scheme

    def __init__(self, url: str, session: ClientSession, db: DB):
        self._scheme = self.get_scheme(url)
        self._base_domain = self.get_base_domain(url)
        self._scrapped_urls = set()
        self._known_urls = set()
        self._session = session
        self._db = db
        self._data = []

    def clear_message(self):
        sys.stdout.write('\b' * len(self._message))
        sys.stdout.flush()

    def print_message(self):
        self.clear_message()
        self._message = f'{self._done}/{self._total} ({self._done/self._total:.2%}) tasks done.'
        sys.stdout.write(self._message)
        sys.stdout.flush()

    def is_subdomain(self, url: str) -> bool:
        url = urlparse(url)
        if not url.netloc.endswith(self._base_domain):
            return False
        prefix = url.netloc[:-len(self._base_domain)]
        if not prefix:
            return True
        return prefix[-1] == '.'

    def is_scrapped(self, url: str) -> bool:
        return url in self._scrapped_urls

    def is_known(self, url: str) -> bool:
        return url in self._known_urls

    async def get_content(self, url: str) -> Union[str, None]:
        for attempt in range(self.MAX_ATTEMPTS):
            try:
                async with self._session.head(url) as head:
                    content_type = head.headers.get('Content-Type', '')
                    if not content_type.startswith('text/html'):
                        return None
                async with self._session.get(url) as response:
                    try:
                        return await response.text()
                    except UnicodeDecodeError:
                        return None
            except (ServerConnectionError, ClientOSError) as err:
                if attempt < self.MAX_ATTEMPTS:
                    await sleep(0.5)

    async def flush(self):
        if len(self._data):
            flushed_data = self._data
            self._data = []
            await self._db.add_URLs(flushed_data)

    async def scrape(self, url: str, depth: int = 0):

        if self.is_scrapped(url):
            self._done += 1
            return

        self._scrapped_urls.add(url)
        self._known_urls.discard(url)

        content = await self.get_content(url)

        if not content:
            self._done += 1
            return

        soup = BeautifulSoup(content, 'lxml')
        title = soup.title
        if title is None:
            title = ''
        else:
            title = title.text
        
        self._data.append((url, title, content))

        if len(self._data) >= self.FLUSH_SIZE:
            await self.flush()

        if depth > 0:

            tasks = []
            for tag in soup.find_all('a'):

                href = tag.get('href')

                if not href:
                    continue

                if href.startswith('/'):
                    href = self.rel2abs(href, url)
                elif not self.is_subdomain(href):
                    continue

                href = self.doctor(href)

                if self.is_scrapped(href) or self.is_known(href):
                    continue

                self._known_urls.add(href)

                tasks.append(Task(self.scrape(href, depth-1)))
                self._total += 1

            if tasks:
                await gather(*tasks)

        self._done += 1

        self.print_message()
