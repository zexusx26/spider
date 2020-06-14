import asyncio
from typing import Union, List, Tuple, Iterator

import asyncpg


class DB:

    _user: str
    _password: str
    _database: str
    _host: str
    _port: int
    _pool: asyncpg.pool.Pool = None

    def __init__(self, user: str, password: str, database: str, host: str = '127.0.0.1', port: int = 5432):
        self._user = user
        self._password = password
        self._database = database
        self._host = host
        self._port = port
        
    async def connect(self):
        if self._pool is not None:\
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
        await self._pool.close()
        self._pool = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS scrapped_data (
            url text PRIMARY KEY,
            title text,
            html text
        )
        """
        async with self._pool.acquire() as conn:
            return await conn.execute(query)

    async def get_URL(self, url: str) -> Union[asyncpg.Record, None]:
        query = """
        SELECT *
        FROM scrapped_data
        WHERE url = $1
        """
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, url)

    async def get_URLs(self, base_domain: str, limit: int = 50, offset: int = 0):# -> Iterator[asyncpg.Record]:
        query = f"""
        SELECT url, title
        FROM scrapped_data
        WHERE url LIKE '%{base_domain}%'
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                cursor = await conn.cursor(query)
                if offset:
                    await cursor.forward(offset)
                for _ in range(limit):
                    record = await cursor.fetchrow()
                    if record:
                        yield record
                    else:
                        return

    async def add_URL(self, url: str, title: str, html: str):
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
            return await conn.fetchrow(query, url, title, html)

    async def add_URLs(self, data: List[Tuple[str, str, str]]):
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
            return await conn.executemany(query, data)

    async def truncate(self):
        query = """
        TRUNCATE TABLE scrapped_data
        """
        async with self._pool.acquire() as conn:
            await conn.execute(query)

"""
postgres=# CREATE DATABASE scrapping;
CREATE DATABASE
postgres=# CREATE ROLE scrapper LOGIN PASSWORD 'password';
CREATE ROLE
postgres=# GRANT ALL PRIVILEGES ON DATABASE scrapping TO scrapper;
GRANT
"""

async def main():

    async with DB('scrapper', 'password', 'scrapping') as db:
        await db.truncate()
        await db.create_table()
        data = await db.get_URL('https://yandex.ru')
        print(data)
        await db.add_URL('https://yandex.ru', 'Яндекс', 'HTML')
        data = await db.get_URL('https://yandex.ru')
        print(data)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

