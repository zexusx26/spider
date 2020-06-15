from typing import List, Tuple, Dict, Callable


class DBMock:

    records: List[Tuple[str, str, str]]

    def __init__(self):
        self.records = []

    async def add_records(self, data: List[Tuple[str, str, str]]):
        self.records += list(data)


class RequestMock:  

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return


class HeaderMock(RequestMock):

    headers: Dict[str, str]

    def __init__(self, headers: Dict[str, str]):
        self.headers = headers

    
class GetMock(RequestMock):

    _text: str
    _text_action: Callable

    def __init__(self, text: str = None, text_action: Callable = None):
        self._text = text
        if text_action is None:
            text_action = lambda get_mock: get_mock._text
        self._text_action = text_action

    async def text(self):
        return self._text_action(self)


class SessionMock:

    heads = Dict[str, Dict[str, str]]
    contents = Dict[str, Tuple[str, Callable]]

    def __init__(self, heads: Dict[str, Dict[str, str]], contents: Dict[str, Tuple[str, Callable]] = None):
        self.heads = heads
        self.contents = contents

    def head(self, url: str, timeout: int) -> HeaderMock:
        assert url in self.heads
        return HeaderMock(self.heads[url])

    def get(self, url: str, timeout: int) -> GetMock:
        assert url in self.contents
        return GetMock(self.contents[url])
