import asyncio

import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import MQTTMemoryTestcaseConfig, MQTTTestcaseConfig

_SKIP_V311 = "request/reply not supported in MQTT 3.1.1 without explicit reply_to"


class Mid(BaseMiddleware):
    async def consume_scope(self, call_next, msg):
        msg.body *= 4
        return await call_next(msg)


@pytest.mark.asyncio()
class MQTTRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs):
        return Mid

    def _skip_if_v311(self):
        if getattr(self, "version", "5.0") == "3.1.1":
            pytest.skip(_SKIP_V311)

    async def test_request_timeout(self, queue):
        self._skip_if_v311()
        await super().test_request_timeout(queue)

    async def test_broker_base_request(self, queue):
        self._skip_if_v311()
        await super().test_broker_base_request(queue)

    async def test_publisher_base_request(self, queue):
        self._skip_if_v311()
        await super().test_publisher_base_request(queue)

    async def test_router_publisher_request(self, queue):
        self._skip_if_v311()
        await super().test_router_publisher_request(queue)

    async def test_broker_request_respect_middleware(self, queue):
        self._skip_if_v311()
        await super().test_broker_request_respect_middleware(queue)

    async def test_broker_publisher_request_respect_middleware(self, queue):
        self._skip_if_v311()
        await super().test_broker_publisher_request_respect_middleware(queue)

    async def test_router_publisher_request_respect_middleware(self, queue):
        self._skip_if_v311()
        await super().test_router_publisher_request_respect_middleware(queue)

    async def test_skip_none_request(self, queue: str) -> None:
        """A `None` request is skipped before the producer is reached.

        With `skip_none` enabled, `request(None)` must return `None`
        immediately and never invoke the subscriber handler.
        """
        self._skip_if_v311()

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

    async def test_request_without_skip_none_returns_response(
        self,
        queue: str,
    ) -> None:
        """Guard the default path: without `skip_none` RPC still works.

        Explicitly pins the flag to `False` so a skip-guard regression is
        caught here.
        """
        self._skip_if_v311()

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


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestRealRequests(MQTTTestcaseConfig, MQTTRequestsTestcase):
    pass


@pytest.mark.mqtt()
class TestRequestTestClient(MQTTMemoryTestcaseConfig, MQTTRequestsTestcase):
    pass
