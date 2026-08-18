import asyncio

import pytest

from faststream import BaseMiddleware
from faststream.redis import BinaryMessageFormatV1, ListSub
from tests.brokers.base.requests import RequestsTestcase

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        data, headers = BinaryMessageFormatV1.parse(self.msg["data"])
        data *= 2
        self.msg["data"] = await BinaryMessageFormatV1.encode(
            message=data,
            reply_to=None,
            correlation_id=headers["correlation_id"],
            headers=headers,
        )

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class RedisRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs):
        return Mid


@pytest.mark.connected()
@pytest.mark.redis()
class TestRealRequests(RedisTestcaseConfig, RedisRequestsTestcase):
    pass


@pytest.mark.redis()
class TestRequestTestClient(RedisMemoryTestcaseConfig, RedisRequestsTestcase):
    async def test_skip_none_request(self, queue: str) -> None:
        """A `None` request is skipped before the producer is reached.

        Without `skip_none`, `request(None)` would still send a request with
        a `null` body. With the flag enabled it must return `None`
        immediately and never invoke the subscriber handler.
        """
        broker = self.get_broker()

        called = asyncio.Event()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            called.set()
            return "Response"

        publisher = broker.publisher(queue, skip_none=True)

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(None, timeout=self.timeout)

        assert response is None
        assert not called.is_set()

    async def test_skip_none_batch_request(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        publisher = broker.publisher(list=ListSub(queue, batch=True), skip_none=True)

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(None, timeout=self.timeout)

        assert response is None

    async def test_request_without_skip_none_returns_response(self, queue: str) -> None:
        """Guard the default path: without `skip_none` RPC still works.

        Duplicates `test_publisher_base_request`, but explicitly pins the
        flag to `False` so a skip-guard regression is caught here.
        """
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        publisher = broker.publisher(queue, skip_none=False)

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(None, timeout=self.timeout)

        assert await response.decode() == "Response"
