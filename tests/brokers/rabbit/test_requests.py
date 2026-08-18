import asyncio

import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import RabbitMemoryTestcaseConfig, RabbitTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        self.msg._Message__lock = False
        self.msg.body *= 2

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class RabbitRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs):
        return Mid


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestRealRequests(RabbitTestcaseConfig, RabbitRequestsTestcase):
    pass


@pytest.mark.rabbit()
@pytest.mark.asyncio()
class TestRequestTestClient(RabbitMemoryTestcaseConfig, RabbitRequestsTestcase):
    async def test_skip_none_request(self, queue: str) -> None:
        """A `None` request is skipped before the producer is reached.

        With `skip_none` enabled, `request(None)` must return `None`
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

    async def test_request_without_skip_none_returns_response(self, queue: str) -> None:
        """Guard the default path: without `skip_none` RPC still works.

        Explicitly pins the flag to `False` so a skip-guard regression is
        caught here.
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
