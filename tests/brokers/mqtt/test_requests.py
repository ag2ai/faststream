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


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestRealRequests(MQTTTestcaseConfig, MQTTRequestsTestcase):
    pass


@pytest.mark.mqtt()
class TestRequestTestClient(MQTTMemoryTestcaseConfig, MQTTRequestsTestcase):
    pass
