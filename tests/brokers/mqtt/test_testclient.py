import asyncio

import pytest

from faststream.exceptions import SetupError
from faststream.mqtt import QoS
from faststream.mqtt.broker.broker import MQTTBroker
from faststream.mqtt.testing import FakeProducer, TestMQTTBroker, mqtt_topic_matches
from tests.brokers.base.testclient import BrokerTestclientTestcase
from tests.marks import require_aiokafka

from .basic import MQTTMemoryTestcaseConfig

_SKIP_V311 = "not supported in MQTT 3.1.1"


@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestTestclient(MQTTMemoryTestcaseConfig, BrokerTestclientTestcase):
    def get_fake_producer_class(self) -> type:
        return FakeProducer

    async def test_consume_with_filter(
        self, queue, mock, event: asyncio.Event, event2: asyncio.Event
    ):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_consume_with_filter(queue, mock, event, event2)

    async def test_response(self, queue, mock, event: asyncio.Event):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_response(queue, mock, event)

    async def test_custom_id_generator(self, queue, mock):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_custom_id_generator(queue, mock)

    async def test_reply_to(self, queue, mock, event: asyncio.Event):
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_reply_to(queue, mock, event)

    async def test_subscriber_assertion_checks_body_fields_and_context(
        self, queue: str
    ) -> None:
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_subscriber_assertion_checks_body_fields_and_context(queue)

    async def test_publisher_assert_called_once_with(self, queue: str) -> None:
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_publisher_assert_called_once_with(queue)

    async def test_subscriber_assert_called_with_reads_the_last_call(
        self, queue: str
    ) -> None:
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_subscriber_assert_called_with_reads_the_last_call(queue)

    async def test_subscriber_assert_any_call_searches_every_call(
        self, queue: str
    ) -> None:
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_subscriber_assert_any_call_searches_every_call(queue)

    async def test_publisher_assertions_share_the_recorded_calls(
        self, queue: str
    ) -> None:
        if self.version == "3.1.1":
            pytest.skip(_SKIP_V311)
        await super().test_publisher_assertions_share_the_recorded_calls(queue)

    async def test_assertions_take_the_mqtt_fields(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(f"{queue}/+")
        async def handle(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", f"{queue}/1", qos=QoS.AT_LEAST_ONCE)

            await handle.assert_called_once_with(
                "hello",
                topic=f"{queue}/1",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
            )
            await handle.assert_called_with(topic=f"{queue}/1")
            await handle.assert_any_call(qos=QoS.AT_LEAST_ONCE, retain=False)

            with pytest.raises(
                AssertionError,
                match=r"qos: expected <QoS.EXACTLY_ONCE: 2>, got <QoS.AT_LEAST_ONCE: 1>",
            ):
                await handle.assert_called_once_with("hello", qos=QoS.EXACTLY_ONCE)

    async def test_publisher_assertions_take_the_mqtt_fields(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue + "2")

        @broker.subscriber(queue)
        async def handle(msg) -> None:
            await publisher.publish("response", qos=QoS.AT_LEAST_ONCE, retain=True)

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)

            await publisher.assert_called_once_with(
                "response",
                topic=queue + "2",
                qos=QoS.AT_LEAST_ONCE,
                retain=True,
            )
            await publisher.assert_called_with(topic=queue + "2")
            await publisher.assert_any_call(qos=QoS.AT_LEAST_ONCE, retain=True)

    @require_aiokafka
    async def test_mqtt_fields_refuse_another_brokers_message(self, queue: str) -> None:
        from faststream.kafka import KafkaBroker, TestKafkaBroker

        broker = self.get_broker()
        kafka = KafkaBroker()

        # The first decorator decides the wrapper class: MQTT's here
        @kafka.subscriber(queue)
        @broker.subscriber(queue)
        async def handle(msg) -> None: ...

        async with self.patch_broker(broker), TestKafkaBroker(kafka):
            await kafka.publish("hello", queue)

            await handle.assert_called_once_with("hello")

            with pytest.raises(SetupError, match="`qos` is a MQTT field"):
                await handle.assert_called_once_with("hello", qos=QoS.AT_MOST_ONCE)

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
