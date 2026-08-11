import gc

import pytest

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
