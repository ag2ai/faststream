import pytest

from faststream.exceptions import FeatureNotSupportedException
from tests.brokers.mqtt.basic import MQTTMemoryTestcaseConfig


@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestV311Restrictions(MQTTMemoryTestcaseConfig):
    @pytest.fixture()
    def mqtt_version(self) -> str:
        return "3.1.1"

    async def test_broker_publish_with_headers_raises(self) -> None:
        broker = self.get_broker()

        with pytest.raises(FeatureNotSupportedException, match="headers"):
            await broker.publish(
                "msg",
                "topic",
                headers={"x": "1"},
            )

    async def test_publisher_publish_with_headers_raises(self) -> None:
        broker = self.get_broker()
        publisher = broker.publisher("topic")

        with pytest.raises(FeatureNotSupportedException, match="headers"):
            await publisher.publish(
                "msg",
                headers={"x": "1"},
            )

    async def test_broker_request_requires_implicitly_reply_to(self) -> None:
        broker = self.get_broker()

        with pytest.raises(
            FeatureNotSupportedException, match="requires an explicit reply_to topic"
        ):
            await broker.request(
                "msg",
                "topic",
            )

    async def test_publisher_request_requires_implicitly_reply_to(self) -> None:
        broker = self.get_broker()
        publisher = broker.publisher("topic")

        with pytest.raises(
            FeatureNotSupportedException, match="requires an explicit reply_to topic"
        ):
            await publisher.request("msg")
