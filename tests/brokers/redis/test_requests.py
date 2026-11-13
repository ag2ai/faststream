from collections.abc import Awaitable, Callable
from typing import Any

import anyio
import pytest

from faststream import BaseMiddleware
from faststream.redis import BinaryMessageFormatV1, RedisBroker
from tests.brokers.base.requests import RequestsTestcase

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        assert self.msg
        data, headers = BinaryMessageFormatV1.parse(self.msg["data"])
        data *= 2
        self.msg["data"] = await BinaryMessageFormatV1.encode(
            message=data,
            reply_to=None,
            correlation_id=headers["correlation_id"],
            headers=headers,
        )

    async def consume_scope(
        self, call_next: Callable[[Any], Awaitable[Any]], msg: Any
    ) -> Any:
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class RedisRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs: Any) -> Any:
        return Mid

    async def test_list_publisher_request(self, queue: str) -> None:
        broker: RedisBroker = self.get_broker()

        publisher = broker.publisher(list=queue)

        @broker.subscriber(list=queue)
        async def handler(msg: Any) -> str:
            return "Response"

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(None, timeout=self.timeout)

        assert await response.decode() == "Response"


@pytest.mark.connected()
@pytest.mark.redis()
class TestRealRequests(RedisTestcaseConfig, RedisRequestsTestcase):
    async def test_request_without_timeout_waits_for_reply(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue)
        async def handler(msg: Any) -> str:
            await anyio.sleep(0.1)
            return "Response"

        async with self.patch_broker(broker):
            await broker.start()

            response = await broker.request(None, queue, timeout=None)

        assert await response.decode() == "Response"


@pytest.mark.redis()
class TestRequestTestClient(RedisMemoryTestcaseConfig, RedisRequestsTestcase):
    pass
