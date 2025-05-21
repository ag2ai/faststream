import inspect
from functools import wraps
from typing import Callable, Protocol, TypeVar
from unittest.mock import MagicMock

from typing_extensions import ParamSpec

P = ParamSpec("P")
T = TypeVar("T")


class SmartMock(Protocol[P, T]):
    mock: MagicMock

    def __call__(self, *args: P.args, **kwds: P.kwargs) -> T: ...


def spy_decorator(method: Callable[P, T]) -> SmartMock[P, T]:
    mock = MagicMock()

    if inspect.iscoroutinefunction(method):

        @wraps(method)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            mock(*args, **kwargs)
            return await method(*args, **kwargs)

    else:

        @wraps(method)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            mock(*args, **kwargs)
            return method(*args, **kwargs)

    wrapper.mock = mock
    return wrapper
