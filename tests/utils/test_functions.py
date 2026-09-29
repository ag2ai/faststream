import traceback
from typing import Any

import pytest

from faststream._internal.utils.functions import FakeContext, call_or_await


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


@pytest.mark.asyncio()
async def test_fake_context_keeps_traceback() -> None:
    error_msg = "boom"
    with pytest.raises(ValueError, match=error_msg) as sync_exc, FakeContext():
        raise ValueError(error_msg)

    with pytest.raises(ValueError, match=error_msg) as async_exc:
        async with FakeContext():
            raise ValueError(error_msg)

    for exc in (sync_exc, async_exc):
        frames = {f.name for f in traceback.extract_tb(exc.tb)}
        assert frames.isdisjoint({"__exit__", "__aexit__"})
