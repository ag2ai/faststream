import gc
from collections.abc import Callable
from functools import wraps
from typing import Any

import pytest

from faststream._internal.di import FastDependsConfig
from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper


@pytest.mark.anyio()
@pytest.mark.parametrize("anyio_backend", ("asyncio",))
async def test_handler_exception_does_not_leak_to_event_loop(
    anyio_backend: str,
) -> None:
    async def handler() -> None:
        return None

    wrapper = HandlerCallWrapper(handler)
    wrapper.set_test()
    error = ValueError("handler failed")

    wrapper.trigger(error=error)
    del wrapper
    gc.collect()


@pytest.mark.asyncio()
async def test_handler_exception_remains_available_to_wait_call() -> None:
    async def handler() -> None:
        return None

    wrapper = HandlerCallWrapper(handler)
    wrapper.set_test()
    error = ValueError("handler failed")

    wrapper.trigger(error=error)

    with pytest.raises(ValueError, match="handler failed") as exc_info:
        await wrapper.wait_call()

    assert exc_info.value is error


def test_composing_twice_decorates_once() -> None:
    async def handler() -> None:
        return None

    def layer(call: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(call)
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            return await call(*args, **kwargs)

        wrapped.layers = getattr(call, "layers", 0) + 1  # type: ignore[attr-defined]
        return wrapped

    wrapper = HandlerCallWrapper(handler)
    for _ in range(2):
        wrapper.set_wrapped(
            dependencies=(),
            _call_decorators=(layer,),
            config=FastDependsConfig(),
        )

    # The old name still reads, and answers the composed call
    assert wrapper._original_call.layers == 1  # type: ignore[attr-defined]
