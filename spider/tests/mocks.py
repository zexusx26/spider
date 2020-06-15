from typing import List, Tuple, Dict, Callable, Union
from abc import ABC


class DBMock:

    records: List[Tuple[str, str, str]]

    def __init__(self):
        self.records = []

    async def add_records(self, data: List[Tuple[str, str, str]]):
        self.records += list(data)


class AsyncContextManagerInterface(ABC):

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return


class HeaderMock(AsyncContextManagerInterface):

    headers: Dict[str, str]

    def __init__(self, headers: Dict[str, str]):
        self.headers = headers


class GetMock(AsyncContextManagerInterface):

    text_value: str
    text_action: Callable

    def __init__(self, text_value: str = None, text_action: Callable = None):
        self.text_value = text_value
        self.text_action = text_action

    async def text(self):
        if self.text_action:
            self.text_action()
        else:
            return self.text_value


HEAD_ACTION = 'head_action'
HEAD_VALUE = 'head_value'
GET_ACTION = 'get_action'
TEXT_ACTION = 'text_action'
TEXT_VALUE = 'text_value'


class SessionMock:

    urls: Dict[str, Dict[str, Union[str, Callable]]]

    def __init__(self, urls: Dict[str, Dict[str, Union[str, Callable]]]):
        self.urls = urls

    def head(self, url: str, timeout: int) -> HeaderMock:
        assert url in self.urls
        url = self.urls[url]
        if HEAD_ACTION in url:
            return url[HEAD_ACTION]()
        else:
            return HeaderMock(url.get(HEAD_VALUE))

    def get(self, url: str, timeout: int) -> GetMock:
        assert url in self.urls
        url = self.urls[url]
        if GET_ACTION in url:
            return url[GET_ACTION]()
        else:
            return GetMock(url.get(TEXT_VALUE), url.get(TEXT_ACTION))
