import asyncio
from unittest.mock import MagicMock

import pytest

from faststream import Path
from faststream.exceptions import SetupError
from faststream.mqtt.annotations import MQTTMessage

from .basic import MQTTTestcaseConfig


@pytest.mark.connected()
@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestPathExtraction(MQTTTestcaseConfig):
    async def test_single_level(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(f"{queue}/devices/{{device_id}}/temperature")
        async def handler(
            body: str,
            device_id: str = Path(),
        ) -> None:
            mock(body, device_id)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("temp-22", f"{queue}/devices/abc/temperature")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("temp-22", "abc")

    async def test_multiple_single_levels(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(f"{queue}/devices/{{device_id}}/{{metric}}")
        async def handler(
            device_id: str = Path(),
            metric: str = Path(),
        ) -> None:
            mock(device_id, metric)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("ignored", f"{queue}/devices/abc/humidity")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("abc", "humidity")

    async def test_raw_plus_with_named_capture(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(f"{queue}/+/devices/{{device_id}}")
        async def handler(device_id: str = Path()) -> None:
            mock(device_id)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", f"{queue}/tenant/devices/abc")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("abc")

    async def test_router_path_with_prefix(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        broker = self.get_broker(apply_types=True)
        router = self.get_router(prefix=f"{queue}/")

        @router.subscriber("devices/{device_id}/temperature")
        async def handler(device_id: str = Path()) -> None:
            mock(device_id)
            event.set()

        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", f"{queue}/devices/prefixed/temperature")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("prefixed")

    async def test_raw_hash_topic_access_via_raw_message(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(f"{queue}/logs/#")
        async def handler(msg: MQTTMessage) -> None:
            mock(msg.raw_message.topic)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("entry", f"{queue}/logs/system/errors")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with(f"{queue}/logs/system/errors")

    async def test_escaped_braces_are_literal_topic(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        broker = self.get_broker(apply_types=True)

        subscriber = broker.subscriber(f"{queue}/root/{{{{braced}}}}")

        @subscriber
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        assert subscriber.topic == f"{queue}/root/{{braced}}"

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("entry", f"{queue}/root/{{braced}}")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("entry")

    async def test_named_capture_with_raw_hash(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(f"{queue}/devices/{{device_id}}/#")
        async def handler(
            msg: MQTTMessage,
            device_id: str = Path(),
        ) -> None:
            mock(device_id, msg.raw_message.topic)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("entry", f"{queue}/devices/abc/logs/system")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("abc", f"{queue}/devices/abc/logs/system")

    @pytest.mark.parametrize(
        "topic",
        (
            "{queue}/devices/{device_id}/metrics/{device_id}",
            "{queue}/devices/pre{device_id}",
            "{queue}/devices/{device_id}post",
            "{queue}/devices/pre{device_id}post",
        ),
    )
    async def test_invalid_templates_raise(
        self,
        queue: str,
        topic: str,
    ) -> None:
        broker = self.get_broker()

        with pytest.raises(SetupError):
            broker.subscriber(topic.replace("{queue}", queue))(lambda: None)
