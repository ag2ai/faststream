import asyncio

import pytest

from faststream.mq.broker.router import MQPublisher, MQRoute
from tests.brokers.base.router import RouterTestcase
from tests.marks import require_ibmmq

from .basic import MQMemoryTestcaseConfig


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestRouter(MQMemoryTestcaseConfig, RouterTestcase):
    route_class = MQRoute
    publisher_class = MQPublisher

    @require_ibmmq
    async def test_iterator_respect_decoder(self, mock, queue):
        await super().test_iterator_respect_decoder(mock, queue)

    @require_ibmmq
    async def test_get_one_respect_decoder(self, queue, event, mock):
        await super().test_get_one_respect_decoder(queue, event, mock)

    async def test_include_publisher_with_prefix(self, queue: str, event) -> None:
        broker = self.get_broker()

        args2, kwargs2 = self.get_subscriber_params(f"test_{queue}")

        @broker.subscriber(*args2, **kwargs2)
        async def handler(m) -> None:
            event.set()

        router = self.get_router()
        publisher = router.publisher(queue)
        broker.include_router(router, prefix="test_")

        async with self.patch_broker(broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(publisher.publish("hello")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
