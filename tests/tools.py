from __future__ import annotations

import inspect
from functools import wraps
from typing import TYPE_CHECKING, Any, Protocol, TypeVar
from unittest.mock import AsyncMock

import pytest
from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from collections.abc import Callable

    from fastapi import FastAPI

P = ParamSpec("P")
T = TypeVar("T")


class SmartMock(Protocol[P, T]):
    mock: AsyncMock

    def __call__(self, *args: P.args, **kwds: P.kwargs) -> T: ...


def spy_decorator(method: Callable[P, T]) -> SmartMock[P, T]:
    mock = AsyncMock()

    if inspect.iscoroutinefunction(method):

        @wraps(method)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            await mock(*args, **kwargs)
            return await method(*args, **kwargs)

    else:

        @wraps(method)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            with pytest.warns(
                RuntimeWarning,
                match="coroutine 'AsyncMockMixin._execute_mock_call' was never awaited",
            ):
                mock(*args, **kwargs)
            return method(*args, **kwargs)

    wrapper.mock = mock
    return wrapper


async def run_lifespan(app: FastAPI) -> dict[str, Any]:
    messages = iter((
        {"type": "lifespan.startup"},
        {"type": "lifespan.shutdown"},
    ))
    scope: dict[str, Any] = {"type": "lifespan", "state": {}}

    async def receive() -> dict[str, str]:
        return next(messages)

    async def send(message: dict[str, str]) -> None: ...

    await app(scope, receive, send)

    return scope["state"]
