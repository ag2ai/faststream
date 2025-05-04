import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import NatsMemoryTestcaseConfig, NatsTestcaseConfig


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        self.msg.data *= 2

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class NatsRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs):
        return Mid

    async def test_broker_stream_request(self, queue: str) -> None:
        broker = self.get_broker()

        stream_name = f"{queue}st"

        args, kwargs = self.get_subscriber_params(queue, stream=stream_name)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        async with self.patch_broker(broker):
            await broker.start()

            response = await broker.request(
                None,
                queue,
                correlation_id="1",
                stream=stream_name,
                timeout=self.timeout,
            )

        assert await response.decode() == "Response"
        assert response.correlation_id == "1"

    async def test_publisher_stream_request(self, queue: str) -> None:
        broker = self.get_broker()

        stream_name = f"{queue}st"
        publisher = broker.publisher(queue, stream=stream_name)

        args, kwargs = self.get_subscriber_params(queue, stream=stream_name)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(
                None,
                correlation_id="1",
                timeout=self.timeout,
            )

        assert await response.decode() == "Response"
        assert response.correlation_id == "1"


@pytest.mark.nats()
class TestRealRequests(NatsTestcaseConfig, NatsRequestsTestcase):
    pass


class TestRequestTestClient(NatsMemoryTestcaseConfig, NatsRequestsTestcase):
    pass
