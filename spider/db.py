from typing import List, Tuple, AsyncIterator, Union

import asyncpg


class DB:

    _user: str                       # имя пользователя БД
    _password: str                   # пароль пользователя БД
    _database: str                   # имя БД
    _host: str                       # хост БД
    _port: int                       # порт БД
    _pool: asyncpg.pool.Pool = None  # пул подключений БД

    def __init__(self, user: str, password: str, database: str, host: str = '127.0.0.1', port: int = 5432):
        """ Инициализация клиента.

        :param user: имя пользователя БД
        :type user: str
        :param password: пароль пользователя БД
        :type password: str
        :param database: имя БД
        :type database: str
        :param host: хост БД, defaults to '127.0.0.1'
        :type host: str, optional
        :param port: порт БД, defaults to 5432
        :type port: int, optional
        """
        self._user = user
        self._password = password
        self._database = database
        self._host = host
        self._port = port

    async def connect(self):
        """ Подключиться к БД и сформировать пул подключений.
        """
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            user=self._user,
            password=self._password,
            database=self._database,
            host=self._host,
            port=self._port,
            min_size=2,
            max_size=5
        )

    async def close(self):
        """ Отключиться от БД и забыть пул подключений. """
        await self._pool.close()
        self._pool = None

    async def __aenter__(self):
        """ При входе в контекстный менеджер подключиться к БД и сформировать пул подключений.

        :return: [description]
        :rtype: [type]
        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """ При выходе из контекстного менеджера отключиться от БД и забыть пул подключений. """
        await self.close()

    async def get_records(self, base_domain: str, limit: int = 50, offset: int = 0) -> AsyncIterator[asyncpg.Record]:
        """ Получить записи из БД.

        Из БД будут извлечены записи, такие, что базовый домен является подстрокой значения поля url записей.

        :param base_domain: базовый URL.
        :type base_domain: str
        :param limit: ограничение числа записей, defaults to 50
        :type limit: int, optional
        :param offset: смещение, defaults to 0
        :type offset: int, optional
        :yield: asyncpg.Record
        :rtype: Iterator[asyncpg.Record]
        """
        query = """
        SELECT url, title
        FROM scrapped_data
        WHERE url LIKE $1
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                cursor = await conn.cursor(query, f'%{base_domain}%')
                if offset:
                    await cursor.forward(offset)
                for _ in range(limit):
                    record = await cursor.fetchrow()
                    if record:
                        yield record
                    else:
                        return

    async def add_records(self, data: List[Tuple[str, str, str]]):
        """ Добавить записи в БД.

        :param data: список кортежей, первый элемент в которых - URl, второй - заголовок, а третий - контент
        :type data: List[Tuple[str, str, str]]
        """
        query = """
        INSERT INTO scrapped_data
        (url, title, html)
        VALUES ($1, $2, $3)
        ON CONFLICT (url)
        DO UPDATE SET
        title = $2,
        html = $3
        """
        async with self._pool.acquire() as conn:
            await conn.executemany(query, data)

    async def execute(self, query: str, *args) -> Union[List[asyncpg.Record], None]:
        """ Выполнить запрос.

        :param query: запрос
        :type query: str
        :return: результат запроса
        :rtype: Union[List[asyncpg.Record], None]
        """
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)
