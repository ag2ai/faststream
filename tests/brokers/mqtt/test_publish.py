import asyncio
from typing import Any

import pytest

from faststream import Context
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import MQTTTestcaseConfig

_SKIP_V311 = "not supported in MQTT 3.1.1"


@pytest.mark.connected()
@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestPublish(MQTTTestcaseConfig, BrokerPublishTestcase):
    async def test_response(self, queue, mock, event: asyncio.Event):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_response(queue, mock, event)

    async def test_reply_to(self, queue, mock, event: asyncio.Event):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_reply_to(queue, mock, event)

    async def test_custom_id_generator(self, queue, mock):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_custom_id_generator(queue, mock)

    async def test_publish_none_is_not_skipped_by_default(self, queue: str) -> None:
        """Guard the default path: without `skip_none` `None` is published."""
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        @pub_broker.subscriber(queue)
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(msg.raw_message.payload)

        publisher = pub_broker.publisher(queue)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await publisher.publish(None)
            value = await asyncio.wait_for(values.get(), timeout=self.timeout)

        assert value == b""

    async def test_publisher_skips_none(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        @pub_broker.subscriber(queue)
        async def handler(msg: Any = Context("message")) -> None:
            await values.put(msg.raw_message.payload)

        publisher = pub_broker.publisher(queue, skip_none=True)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            result = await publisher.publish(None)

            assert result is None

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(values.get(), timeout=1.0)

    async def test_handler_return_none_is_skipped(self, queue: str) -> None:
        pub_broker = self.get_broker(apply_types=True)

        values: asyncio.Queue[bytes | None] = asyncio.Queue()

        @pub_broker.publisher(queue + "1", skip_none=True)
        @pub_broker.subscriber(queue)
        async def handler(msg: Any = Context("message")) -> None: ...

        @pub_broker.subscriber(queue + "1")
        async def out_handler(msg: Any = Context("message")) -> None:
            await values.put(msg.raw_message.payload)

        async with self.patch_broker(pub_broker) as br:
            await br.start()
            await br.publish("test", queue)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(values.get(), timeout=1.0)
