import asyncio
from unittest.mock import Mock

import pytest

from faststream import Context
from faststream.confluent import KafkaResponse
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import ConfluentTestcaseConfig


@pytest.mark.confluent()
class TestPublish(ConfluentTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_publish_batch(self, queue: str) -> None:
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await br.publish_batch(1, "hi", topic=queue)

            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=self.timeout,
            )

        assert {1, "hi"} == {r.result() for r in result}

    @pytest.mark.asyncio()
    async def test_batch_publisher_manual(self, queue: str) -> None:
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        publisher = pub_broker.publisher(queue, batch=True)

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await publisher.publish(1, "hi")

            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=self.timeout,
            )

        assert {1, "hi"} == {r.result() for r in result}

    @pytest.mark.asyncio()
    async def test_batch_publisher_decorator(self, queue: str) -> None:
        pub_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=2)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        async def handler(msg) -> None:
            await msgs_queue.put(msg)

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @pub_broker.publisher(queue, batch=True)
        @pub_broker.subscriber(*args2, **kwargs2)
        async def pub(m):
            return 1, "hi"

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await br.publish("", queue + "1")

            result, _ = await asyncio.wait(
                (
                    asyncio.create_task(msgs_queue.get()),
                    asyncio.create_task(msgs_queue.get()),
                ),
                timeout=self.timeout,
            )

        assert {1, "hi"} == {r.result() for r in result}

    @pytest.mark.asyncio()
    async def test_response(
        self,
        queue: str,
        mock: Mock,
    ) -> None:
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @pub_broker.subscriber(*args, **kwargs)
        @pub_broker.publisher(topic=queue + "1")
        async def handle():
            return KafkaResponse(1)

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @pub_broker.subscriber(*args2, **kwargs2)
        async def handle_next(msg=Context("message")) -> None:
            mock(body=msg.body)
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout * 1.5,
            )

        assert event.is_set()
        mock.assert_called_once_with(body=b"1")
