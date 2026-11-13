from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock

import anyio
import pytest

from faststream import BaseMiddleware
from faststream.rabbit.publisher.producer import _RPCCallback
from tests.brokers.base.requests import RequestsTestcase

from .basic import RabbitMemoryTestcaseConfig, RabbitTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        assert self.msg
        self.msg._Message__lock = False
        self.msg.body *= 2

    async def consume_scope(
        self, call_next: Callable[[Any], Awaitable[Any]], msg: Any
    ) -> Any:
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class RabbitRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs: Any) -> Any:
        return Mid


# A reply stream left open warns only when it is collected
@pytest.mark.filterwarnings("error::ResourceWarning")
@pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.connected()
@pytest.mark.rabbit()
class TestRealRequests(RabbitTestcaseConfig, RabbitRequestsTestcase):
    pass


@pytest.mark.rabbit()
@pytest.mark.asyncio()
class TestRequestTestClient(RabbitMemoryTestcaseConfig, RabbitRequestsTestcase):
    pass


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_rpc_callback_cleans_up_when_consume_fails() -> None:
    lock = anyio.Lock()
    queue = AsyncMock()
    queue.consume.side_effect = ConnectionError
    callback = _RPCCallback(lock, queue)

    with pytest.raises(ConnectionError):
        async with callback:
            pass

    assert not lock.locked()
    with pytest.raises(anyio.ClosedResourceError):
        callback.receive_response_stream.receive_nowait()


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_rpc_callback_closes_streams_when_cancel_fails() -> None:
    queue = AsyncMock()
    queue.cancel.side_effect = ConnectionError

    with pytest.raises(ConnectionError):
        async with _RPCCallback(anyio.Lock(), queue) as response_stream:
            pass

    with pytest.raises(anyio.ClosedResourceError):
        response_stream.receive_nowait()
