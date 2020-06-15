import sys
from asyncio import Task, gather, sleep, wait_for, TimeoutError
from typing import List, Tuple, Union
from urllib.parse import urljoin, urlparse

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientOSError, ServerConnectionError
from bs4 import BeautifulSoup, Tag


class Scrapper:

    MAX_ATTEMPTS = 3    # максимальное число попыток получения заголовков контента
    SLEEP_TIME = 0.5    # время между попытками подключения
    TIMEOUT = 3         # таймаут подключения
    FLUSH_SIZE = 100    # число записей, при достижении которого данные записываются в базу

    _base_domain: str                   # домен второго уровня, с которым происходит работа
    _scrapped_urls: set                 # множество URl, контент которых получен (или была попытка получения)
    _known_urls: set                    # множество URl, которые находятся в очереди на обработку
    _session: ClientSession             # клиент, отправляющий запросы
    _db: 'DB'                           # клиент БД
    _data: List[Tuple[str, str, str]]   # данные для записи в БД
    _total: int = 1                     # общее число задач
    _done: int = 0                      # число выполненных задач
    _message: str = ''                  # статус-сообщение
    stat: dict                          # словарь со статистикой задач

    @staticmethod
    def doctor(url: str) -> str:
        """ Простая нормализация домена (убирает фрагмент и завершающий слэш).

        :param url: входной URL
        :type url: str
        :return: нормализованный URL
        :rtype: str

        >>> Scrapper.doctor('https://example.com#frag')
        'https://example.com'
        >>> Scrapper.doctor('https://example.com/')
        'https://example.com'
        >>> Scrapper.doctor('https://example.com/#frag')
        'https://example.com'
        >>> Scrapper.doctor('https://example.com')
        'https://example.com'
        """
        if '#' in url:
            url = url[:url.find('#')]
        url = url.strip('/')
        return url

    @staticmethod
    def get_base_domain(url: str) -> str:
        """ Получить домен второго уровня для URL.

        :param url: URL
        :type url: str
        :return: домен второго уровня
        :rtype: str
        
        >>> Scrapper.get_base_domain('https://up.example.com')
        'example.com'
        >>> Scrapper.get_base_domain('https://example.com/')
        'example.com'
        >>> Scrapper.get_base_domain('https://example.com/some/path')
        'example.com'
        >>> Scrapper.get_base_domain('https://example.com/some/path/')
        'example.com'
        >>> Scrapper.get_base_domain('https://example.com/some/path/?key=value')
        'example.com'
        >>> Scrapper.get_base_domain('https://example.com/some/path/?key=value#frag')
        'example.com'
        """
        url = urlparse(url)
        netloc = url.netloc
        return netloc.split('.', netloc.count('.') - 1)[-1]

    @staticmethod
    def rel2abs(path: str, base: str) -> str:
        """ Сформировать абсолютную ссылку из относительной.

        :param path: относительный путь
        :type path: str
        :param base: базовый URL
        :type base: str
        :return: абсолютная ссылка
        :rtype: str
        >>> Scrapper.rel2abs('/some/path', 'https://up.example.com')
        'https://up.example.com/some/path'
        >>> Scrapper.rel2abs('/some/path', 'https://example.com/')
        'https://example.com/some/path'
        >>> Scrapper.rel2abs('/some/path', 'https://example.com/another/path')
        'https://example.com/some/path'
        >>> Scrapper.rel2abs('/some/path', 'https://example.com/another/path/')
        'https://example.com/some/path'
        >>> Scrapper.rel2abs('/some/path', 'https://example.com/another/path/?key=value')
        'https://example.com/some/path'
        >>> Scrapper.rel2abs('/some/path', 'https://example.com/another/path/?key=value#frag')
        'https://example.com/some/path'
        """
        base = urlparse(base)
        base = '://'.join((base.scheme, base.netloc))
        return urljoin(base, path)

    def __init__(self, url: str, session: ClientSession, db: 'DB'):
        """ Инициализация скраппера.

        :param url: URL, с которого начинается обход. На основе этого URL будет получен базовый домен
        :type url: str
        :param session: сессия для формирования запросов
        :type session: ClientSession
        :param db: клиент для работы с БД
        :type db: DB
        """
        self._base_domain = self.get_base_domain(url)
        self._scrapped_urls = set()
        self._known_urls = set()
        self._session = session
        self._db = db
        self._data = []
        self.stat = {}

    def clear_message(self):
        """ Вспомогательный метод для очистки статусного сообщения. """
        sys.stdout.write('\b' * len(self._message))
        sys.stdout.flush()

    def print_message(self):
        """ Вспомогательный метод для вывода статусного сообщения. """
        self.clear_message()
        self._message = f'{self._done}/{self._total} ({self._done/self._total:.2%}) tasks done.'
        sys.stdout.write(self._message)
        sys.stdout.flush()

    def is_subdomain(self, url: str) -> bool:
        """ Проверка того, что URL относится к базовому домену или его поддомену.

        :param url: URL
        :type url: str
        :return: True, если URL относится к базовому домену или его поддомену, иначе False
        :rtype: bool
        """
        url = urlparse(url)
        if not url.netloc.endswith(self._base_domain):
            return False
        prefix = url.netloc[:-len(self._base_domain)]
        if not prefix:
            return True
        return prefix[-1] == '.'

    async def get_content(self, url: str) -> Union[str, None]:
        """ Получить контент, соответствующий URL.

        Если Content-Type страницы не является text/html, то возвращается None.
        Если при получении контента произошла ошибка UnicodeDecodeError, то возвращается None.
        Если после нескольких попыток не удалось получить контент (были вызваны исключения ServerConnectionError или
        ClientOSError), то возвращается None.

        :param url: URL
        :type url: str
        :return: HTML, относящийся к URL или None
        :rtype: Union[str, None]
        """

        # проверяем тип контента
        for attempt in range(self.MAX_ATTEMPTS):
            
            try:
                async with self._session.head(url, timeout=self.TIMEOUT) as head:
                    content_type = head.headers.get('Content-Type', '')
                    if not content_type.startswith('text/html'):
                        self.stat['wrong_content_type'] = self.stat.get('wrong_content_type', {})
                        wrong_content_type = self.stat['wrong_content_type']
                        wrong_content_type[content_type] = wrong_content_type.get(content_type, 0) + 1
                        return None
                    else:
                        break
            
            except (ServerConnectionError, ClientOSError, TimeoutError):
                if attempt < self.MAX_ATTEMPTS - 1:
                    await sleep(self.SLEEP_TIME)

        else:
            return None

        # если тип контента подходящий, то получаем контент
        for attempt in range(self.MAX_ATTEMPTS):

            try:
                async with self._session.get(url, timeout=self.TIMEOUT) as response:
                    try:
                        return await response.text()
                    except UnicodeDecodeError:
                        self.stat['unicode_decode_error'] = self.stat.get('unicode_decode_error', 0) + 1
                        return None

            except (ServerConnectionError, ClientOSError, TimeoutError):
                if attempt < self.MAX_ATTEMPTS - 1:
                    await sleep(self.SLEEP_TIME)

        self.stat['connection_error'] = self.stat.get('connection_error', 0) + 1
        return None

    async def flush(self):
        """ Записать данные в БД. """
        if len(self._data):
            flushed_data = self._data
            self._data = []
            await self._db.add_records(flushed_data)

    async def scrape(self, url: str, depth: int = 0):
        """ Получить контент страницы.

        Если depth больше единицы, то будут получены ссылки, содержащиеся на странице, ведущие на страницы базового
        домена или его поддоменов. Для этих ссылок будет вызвана эта функция обхода, но с меньшим значением depth.

        :param url: URL
        :type url: str
        :param depth: уровень глубины обхода, defaults to 0
        :type depth: int, optional
        """

        # если данный URL уже посещали, то пропускаем его
        if url in self._scrapped_urls:
            self._done += 1
            self.stat['scrapped'] = self.stat.get('scrapped', 0) + 1
            return

        # переносим URL из множества URL, находящихся в очереди в множество посещенных URL
        self._scrapped_urls.add(url)
        self._known_urls.discard(url)

        # получаем контент
        content = await self.get_content(url)

        # если контент не получен, то завершаем выполнение задачи
        if not content:
            self._done += 1
            return

        # запускаем парсер контента, извлекает заголовок
        soup = BeautifulSoup(content, 'lxml')
        title = soup.title
        if title is None:
            title = ''
        else:
            title = title.text

        # добавляем данные для запись в БД
        self._data.append((url, title, content))

        # если данных достаточно много, записываем из в БД
        if len(self._data) >= self.FLUSH_SIZE:
            await self.flush()

        # если требуется обход в глубину, то парсим ссылки и ставим задачи
        if depth > 0:

            links = (tag for tag in soup.find_all('a'))
            links = (self.check_link(tag, url) for tag in links)
            links = {link for link in links if link is not None}
            links -= self._scrapped_urls
            links -= self._known_urls

            soup = None

            if links:
                self._total += len(links)
                self._known_urls ^= links
                links = (Task(self.scrape(link, depth-1)) for link in links)
                await gather(*links)

        # задача выполнена, обновляем статусное сообщение
        self.stat['done'] = self.stat.get('done', 0) + 1
        self._done += 1
        self.print_message()

    def check_link(self, tag: Tag, url: str) -> Union[str, None]:
        """ Получить текст ссылки тэга.

        :param tag: тэг
        :type tag: Tag
        :param url: URL, на странице которого получена ссылка
        :type url: str
        :return: текст ссылки, если получен, иначе None
        :rtype: Union[str, None]
        """

        link = tag.get('href')

        # если трибут href отсутствует, то возвращаем None
        if not link:
            return None

        # если ссылка относительная, то дополняем ее, если нет - проверяем, что она относится к базовому домену или
        # его поддомену. Если проверка провалена, возвращаем None.
        if link.startswith('/'):
            link = self.rel2abs(link, url)
        elif not self.is_subdomain(link):
            return None

        # нормализуем ссылку
        link = self.doctor(link)

        # возвращаем ссылку
        return link


if __name__ == '__main__':
    import doctest
    doctest.testmod()
