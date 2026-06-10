from typing import Any

import pytest
from typing_extensions import Self

from faststream._internal.testing.ast import is_contains_context_name


class Context:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


class A(Context):
    def __init__(self) -> None:
        self.contains = is_contains_context_name(self.__class__.__name__, B.__name__)


class B(Context):
    def __init__(self) -> None:
        pass


class UserWrapper:
    """Mirrors a user helper that opens the ``with`` block inside ``__init__``.

    The user's ``__init__`` frame is a regular caller frame and must not be
    skipped by context-name detection.
    """

    def __init__(self) -> None:
        with A() as a, B():
            self.contains = a.contains


class UserWrapperInvalid:
    def __init__(self) -> None:
        with B(), A() as a:
            self.contains = a.contains


def test_base() -> None:
    with A() as a, B():
        assert a.contains


@pytest.mark.asyncio()
async def test_base_async() -> None:
    async with A() as a, B():
        assert a.contains


def test_nested() -> None:
    with A() as a, B():
        assert a.contains


@pytest.mark.asyncio()
async def test_nested_async() -> None:
    async with A() as a, B():
        assert a.contains


@pytest.mark.asyncio()
async def test_async_A() -> None:  # noqa: N802
    async with A() as a:
        with B():
            assert a.contains


@pytest.mark.asyncio()
async def test_async_B() -> None:  # noqa: N802
    with A() as a:
        async with B():
            assert a.contains


def test_base_invalid() -> None:
    with B(), B(), A() as a:
        assert not a.contains


def test_nested_invalid() -> None:
    with B(), A() as a:
        assert not a.contains


def test_user_init_frame() -> None:
    assert UserWrapper().contains


def test_user_init_frame_invalid() -> None:
    assert not UserWrapperInvalid().contains


def test_not_broken() -> None:
    with A() as a, B():
        assert a.contains

        # test ast processes another context correctly
        with pytest.raises(ValueError):  # noqa: PT011
            raise ValueError
