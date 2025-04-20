import asyncio
from collections.abc import AsyncIterator, Awaitable, Iterator
from concurrent.futures import Executor
from contextlib import asynccontextmanager, contextmanager
from functools import partial, wraps
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
    cast,
    overload,
)

from fast_depends.core import CallModel
from fast_depends.utils import (
    is_coroutine_callable,
    run_async as call_or_await,
    run_in_threadpool,
)
from typing_extensions import ParamSpec

from faststream._internal.basic_types import F_Return, F_Spec

__all__ = (
    "call_or_await",
    "drop_response_type",
    "fake_context",
    "to_async",
)

P = ParamSpec("P")
T = TypeVar("T")


@overload
def to_async(
    func: Callable[F_Spec, Awaitable[F_Return]],
) -> Callable[F_Spec, Awaitable[F_Return]]: ...


@overload
def to_async(
    func: Callable[F_Spec, F_Return],
) -> Callable[F_Spec, Awaitable[F_Return]]: ...


def to_async(
    func: Union[
        Callable[F_Spec, F_Return],
        Callable[F_Spec, Awaitable[F_Return]],
    ],
) -> Callable[F_Spec, Awaitable[F_Return]]:
    """Converts a synchronous function to an asynchronous function."""
    if is_coroutine_callable(func):
        return cast("Callable[F_Spec, Awaitable[F_Return]]", func)

    func = cast("Callable[F_Spec, F_Return]", func)

    @wraps(func)
    async def to_async_wrapper(*args: F_Spec.args, **kwargs: F_Spec.kwargs) -> F_Return:
        """Wraps a function to make it asynchronous."""
        return await run_in_threadpool(func, *args, **kwargs)

    return to_async_wrapper


@asynccontextmanager
async def fake_context(*args: Any, **kwargs: Any) -> AsyncIterator[None]:
    yield None


@contextmanager
def sync_fake_context(*args: Any, **kwargs: Any) -> Iterator[None]:
    yield None


def drop_response_type(model: CallModel) -> CallModel:
    model.serializer.response_callback = None
    return model


async def return_input(x: Any) -> Any:
    return x


async def run_in_executor(
    executor: Optional[Executor],
    func: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, partial(func, *args, **kwargs))
