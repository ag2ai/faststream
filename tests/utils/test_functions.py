from typing import Any

import pytest

from faststream._internal.utils.functions import call_or_await


def sync_func(a: Any) -> Any:
    return a


async def async_func(a: Any) -> Any:
    return a


@pytest.mark.asyncio()
async def test_call() -> None:
    assert (await call_or_await(sync_func, a=3)) == 3


@pytest.mark.asyncio()
async def test_await() -> None:
    assert (await call_or_await(async_func, a=3)) == 3
