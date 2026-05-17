import pytest

from faststream import BaseMiddleware
from tests.brokers.base.requests import RequestsTestcase

from .basic import MQMemoryTestcaseConfig

VALID_CORRELATION_ID = "01" * 24
VALID_MESSAGE_ID = "02" * 24


class Mid(BaseMiddleware):
    async def on_receive(self) -> None:
        self.msg.body *= 2

    async def consume_scope(self, call_next, msg):
        msg.body *= 2
        return await call_next(msg)


@pytest.mark.asyncio()
class MQRequestsTestcase(RequestsTestcase):
    def get_middleware(self, **kwargs):
        return Mid

    async def test_broker_base_request(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        async with self.patch_broker(broker):
            await broker.start()

            response = await broker.request(
                None,
                queue,
                timeout=self.timeout,
                correlation_id=VALID_CORRELATION_ID,
                message_id=VALID_MESSAGE_ID,
            )

        assert await response.decode() == "Response"
        assert response.correlation_id == VALID_MESSAGE_ID

    async def test_publisher_base_request(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(
                None,
                timeout=self.timeout,
                correlation_id=VALID_CORRELATION_ID,
                message_id=VALID_MESSAGE_ID,
            )

        assert await response.decode() == "Response"
        assert response.correlation_id == VALID_MESSAGE_ID

    async def test_router_publisher_request(self, queue: str) -> None:
        router = self.get_router()

        publisher = router.publisher(queue)

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def handler(msg) -> str:
            return "Response"

        broker = self.get_broker()
        broker.include_router(router)

        async with self.patch_broker(broker):
            await broker.start()

            response = await publisher.request(
                None,
                timeout=self.timeout,
                correlation_id=VALID_CORRELATION_ID,
                message_id=VALID_MESSAGE_ID,
            )

        assert await response.decode() == "Response"
        assert response.correlation_id == VALID_MESSAGE_ID


@pytest.mark.mq()
class TestRequestTestClient(MQMemoryTestcaseConfig, MQRequestsTestcase):
    pass
