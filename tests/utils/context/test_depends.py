from typing import Annotated

import pytest

from faststream import Depends
from faststream._internal.utils import apply_types


def sync_dep(key):
    return key


async def async_dep(key):
    return key


@pytest.mark.asyncio()
async def test_sync_depends() -> None:
    key = 1000

    @apply_types
    def func(k=Depends(sync_dep)):
        return k is key

    assert func(key=key)


@pytest.mark.asyncio()
async def test_sync_with_async_depends() -> None:
    with pytest.raises(AssertionError):

        @apply_types
        def func(k=Depends(async_dep)) -> None:  # pragma: no cover
            pass


@pytest.mark.asyncio()
async def test_async_depends() -> None:
    key = 1000

    @apply_types
    async def func(k=Depends(async_dep)):
        return k is key

    assert await func(key=key)


@pytest.mark.asyncio()
async def test_async_with_sync_depends() -> None:
    key = 1000

    @apply_types
    async def func(k=Depends(sync_dep)):
        return k is key

    assert await func(key=key)


@pytest.mark.asyncio()
async def test_annotated_depends() -> None:
    D = Annotated[int, Depends(sync_dep)]  # noqa: N806

    key = 1000

    @apply_types
    async def func(k: D):
        return k == key

    assert await func(key=key)
