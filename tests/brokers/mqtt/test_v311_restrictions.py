from unittest.mock import AsyncMock, MagicMock

import pytest

from faststream.exceptions import FeatureNotSupportedException
from faststream.mqtt.publisher.producer import ZmqttProducerV5, ZmqttProducerV311
from faststream.mqtt.response import MQTTPublishCommand
from faststream.response.publish_type import PublishType


@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestV311Restrictions:
    """V311 feature checks live in ZmqttProducerV311.publish(); test that directly."""

    async def test_publish_with_headers_raises(self) -> None:
        producer = ZmqttProducerV311(parser=None, decoder=None)
        producer._client = MagicMock()  # bypass connect check

        cmd = MQTTPublishCommand(
            "msg",
            topic="test",
            headers={"x": "1"},
            _publish_type=PublishType.PUBLISH,
        )
        with pytest.raises(FeatureNotSupportedException, match="headers"):
            await producer.publish(cmd)

    async def test_publish_without_headers_ok(self) -> None:
        producer = ZmqttProducerV311(parser=None, decoder=None)
        mock_client = AsyncMock()
        producer._client = mock_client

        cmd = MQTTPublishCommand(
            "msg",
            topic="test",
            _publish_type=PublishType.PUBLISH,
        )
        await producer.publish(cmd)
        mock_client.publish.assert_awaited_once()

    async def test_v5_publish_with_headers_ok(self) -> None:
        producer = ZmqttProducerV5(parser=None, decoder=None)
        mock_client = AsyncMock()
        producer._client = mock_client

        cmd = MQTTPublishCommand(
            "msg",
            topic="test",
            headers={"x": "1"},
            _publish_type=PublishType.PUBLISH,
        )
        await producer.publish(cmd)
        mock_client.publish.assert_awaited_once()
