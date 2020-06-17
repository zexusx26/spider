import asyncio
from functools import wraps
from typing import Callable

import pytest


def async_test(async_func: Callable) -> Callable:

    @wraps(async_func)
    def wrapper(*args, **kwargs):

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(async_func(*args, **kwargs))
        loop.close()

    return wrapper
