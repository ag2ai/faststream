import pytest

from faststream.mqtt.broker.broker import MQTTBroker
from faststream.mqtt.testing import FakeProducer, TestMQTTBroker, mqtt_topic_matches
from tests.brokers.base.testclient import BrokerTestclientTestcase

from .basic import MQTTMemoryTestcaseConfig

_SKIP_V311 = "not supported in MQTT 3.1.1"


@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestTestclient(MQTTMemoryTestcaseConfig, BrokerTestclientTestcase):
    def get_fake_producer_class(self) -> type:
        return FakeProducer

    async def test_consume_with_filter(self, queue, mock):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_consume_with_filter(queue, mock)

    async def test_response(self, queue, mock):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_response(queue, mock)

    async def test_reply_to(self, queue, mock):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_reply_to(queue, mock)

    @pytest.mark.connected()
    async def test_broker_gets_patched_attrs_within_cm(self) -> None:
        await super().test_broker_gets_patched_attrs_within_cm(FakeProducer)

    @pytest.mark.connected()
    async def test_broker_with_real_doesnt_get_patched(self) -> None:
        await super().test_broker_with_real_doesnt_get_patched()

    @pytest.mark.connected()
    async def test_broker_with_real_patches_publishers_and_subscribers(
        self,
        queue: str,
    ) -> None:
        await super().test_broker_with_real_patches_publishers_and_subscribers(queue)


class TestTopicMatching:
    """Unit tests for the MQTT wildcard matching helper."""

    def test_exact_match(self) -> None:
        assert mqtt_topic_matches("sensors/temp", "sensors/temp")

    def test_exact_no_match(self) -> None:
        assert not mqtt_topic_matches("sensors/temp", "sensors/humidity")

    def test_single_level_wildcard(self) -> None:
        assert mqtt_topic_matches("sensors/+/temp", "sensors/room1/temp")
        assert not mqtt_topic_matches("sensors/+/temp", "sensors/room1/floor2/temp")

    def test_multi_level_wildcard(self) -> None:
        assert mqtt_topic_matches("sensors/#", "sensors/room1/temp")
        assert mqtt_topic_matches("sensors/#", "sensors/room1/floor/temp")
        assert mqtt_topic_matches("sensors/#", "sensors")

    def test_root_hash(self) -> None:
        assert mqtt_topic_matches("#", "anything/at/all")

    def test_shared_subscription(self) -> None:
        assert mqtt_topic_matches("$share/workers/sensors/#", "sensors/temp")
        assert not mqtt_topic_matches("$share/workers/sensors/+", "sensors/a/b")


@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestWildcardRouting:
    """Verify that TestMQTTBroker routes wildcard topics correctly."""

    async def test_plus_wildcard_routes(self, queue: str) -> None:
        broker = MQTTBroker()

        @broker.subscriber(f"{queue}/+/temp")
        async def handler(temperature: float) -> None:
            pass

        async with TestMQTTBroker(broker) as br:
            await br.start()
            await br.publish("22.5", f"{queue}/room1/temp")
            handler.mock.assert_called_once_with("22.5")

    async def test_hash_wildcard_routes(self, queue: str) -> None:
        broker = MQTTBroker()

        @broker.subscriber(f"{queue}/#")
        async def handler(msg: str) -> None:
            pass

        async with TestMQTTBroker(broker) as br:
            await br.start()
            await br.publish("data", f"{queue}/a/b/c")
            handler.mock.assert_called_once_with("data")

    async def test_no_match_skips_handler(self, queue: str) -> None:
        broker = MQTTBroker()

        @broker.subscriber(f"{queue}/exact")
        async def handler(msg: str) -> None:
            pass

        async with TestMQTTBroker(broker) as br:
            await br.start()
            await br.publish("data", f"{queue}/other")
            handler.mock.assert_not_called()

    async def test_shared_subscription_routing(self, queue: str) -> None:
        """Messages to a shared group topic should reach exactly one subscriber."""
        broker = MQTTBroker()

        call_count = 0

        @broker.subscriber(f"{queue}/data", shared="workers")
        async def handler(msg: str) -> None:
            nonlocal call_count
            call_count += 1

        async with TestMQTTBroker(broker) as br:
            await br.start()
            await br.publish("hello", f"{queue}/data")
            assert call_count == 1
