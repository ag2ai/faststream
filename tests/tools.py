import inspect
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, Protocol, TypeVar, cast
from unittest.mock import AsyncMock

import pytest
from typing_extensions import ParamSpec

P = ParamSpec("P")
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class SmartMock(Protocol[P, T_co]):
    mock: AsyncMock

    def __call__(self, *args: P.args, **kwds: P.kwargs) -> T_co: ...


def spy_decorator(method: Callable[P, T]) -> SmartMock[P, T]:
    mock = AsyncMock()

    @wraps(method)
    async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
        await mock(*args, **kwargs)
        return await cast("Callable[P, Awaitable[Any]]", method)(*args, **kwargs)

    @wraps(method)
    def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        with pytest.warns(
            RuntimeWarning,
            match="coroutine 'AsyncMockMixin._execute_mock_call' was never awaited",
        ):
            mock(*args, **kwargs)
        return method(*args, **kwargs)

    wrapper = cast(
        "SmartMock[P, T]",
        async_wrapper if inspect.iscoroutinefunction(method) else sync_wrapper,
    )
    wrapper.mock = mock
    return wrapper
