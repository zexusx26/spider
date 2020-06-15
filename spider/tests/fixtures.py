import asyncio
from functools import wraps
from typing import Callable

import pytest


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


def async_test(async_func: Callable) -> Callable:

    @wraps(async_func)
    def wrapper(event_loop, *args, **kwargs):
        event_loop.run_until_complete(async_func(event_loop, *args, **kwargs))

    return wrapper
