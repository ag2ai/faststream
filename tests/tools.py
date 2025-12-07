import inspect
from collections.abc import Callable
from functools import wraps
from typing import Protocol, TypeVar
from unittest.mock import AsyncMock

import pytest
from typing_extensions import ParamSpec

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
