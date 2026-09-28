from typing import Any, Literal

import pytest
from dirty_equals import IsPartialDataclass
from zmqtt import MQTTDisconnectedError

from faststream.middlewares import AckPolicy
from faststream.mqtt import QoS
from faststream.mqtt.broker.broker import MQTTBroker
from tests.brokers.base.connection import BrokerConnectionTestcase

from .settings import Settings


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestConnection(BrokerConnectionTestcase):
    broker = MQTTBroker
    version: Literal["5.0", "3.1.1"] = "3.1.1"

    @pytest.fixture(autouse=True)
    def setup_version(self, mqtt_version: Literal["5.0", "3.1.1"]) -> None:
        self.version = mqtt_version

    def get_broker_args(self, settings: Settings) -> dict[str, Any]:
        return {
            "host": settings.host,
            "port": settings.port,
            "version": self.version,
        }

    @pytest.mark.asyncio()
    async def test_connection_info(self, settings: Settings, queue: str) -> None:
        broker = self.broker(client_id=queue, **self.get_broker_args(settings))

        with pytest.raises(MQTTDisconnectedError):
            _ = broker.connection_info

        async with broker:
            snapshot = broker.connection_info
            assert snapshot == IsPartialDataclass(
                connection_id=1,
                session_present=False,
                effective_client_id=queue,
            )

        with pytest.raises(MQTTDisconnectedError):
            _ = broker.connection_info

        async with broker:
            assert broker.connection_info is not snapshot
            assert await broker.ping()


@pytest.mark.connected()
@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestConnectProperties:
    async def test_broker_connect_properties(
        self, settings: Settings, queue: str
    ) -> None:
        broker = MQTTBroker(
            host=settings.host,
            port=settings.port,
            version="5.0",
            receive_maximum=10,
            maximum_packet_size=4096,
            user_properties=(("role", "worker"), ("role", "reader")),
            request_response_information=True,
            request_problem_information=False,
        )
        subscriber = broker.subscriber(queue, qos=QoS.AT_LEAST_ONCE)

        async with broker:
            await broker.start()
            assert await subscriber.get_one(timeout=0.01) is None
            await broker.publish("accepted", queue, qos=QoS.AT_LEAST_ONCE)
            message = await subscriber.get_one()

        assert message is not None
        assert await message.decode() == "accepted"

    async def test_receive_maximum_waits_for_ack(
        self, settings: Settings, queue: str
    ) -> None:
        broker = MQTTBroker(
            host=settings.host, port=settings.port, version="5.0", receive_maximum=1
        )
        publisher = MQTTBroker(host=settings.host, port=settings.port, version="5.0")
        subscriber = broker.subscriber(
            queue, qos=QoS.AT_LEAST_ONCE, ack_policy=AckPolicy.MANUAL
        )

        async with broker, publisher:
            await broker.start()
            assert await subscriber.get_one(timeout=0.01) is None
            await publisher.publish("first", queue, qos=QoS.AT_LEAST_ONCE)
            await publisher.publish("second", queue, qos=QoS.AT_LEAST_ONCE)

            first = await subscriber.get_one()
            assert first is not None
            assert await subscriber.get_one(timeout=0.2) is None
            await first.ack()

            second = await subscriber.get_one()
            assert second is not None
            await second.ack()
            assert (await first.decode(), await second.decode()) == ("first", "second")

    async def test_maximum_packet_size_limits_incoming_messages(
        self, settings: Settings, queue: str
    ) -> None:
        broker = MQTTBroker(
            host=settings.host, port=settings.port, version="5.0", maximum_packet_size=512
        )
        publisher = MQTTBroker(host=settings.host, port=settings.port, version="5.0")
        subscriber = broker.subscriber(queue, qos=QoS.AT_LEAST_ONCE)

        async with broker, publisher:
            await broker.start()
            assert await subscriber.get_one(timeout=0.01) is None
            await publisher.publish(b"x" * 1024, queue, qos=QoS.AT_LEAST_ONCE)
            await publisher.publish(b"small", queue, qos=QoS.AT_LEAST_ONCE)
            message = await subscriber.get_one()

            assert message is not None
            assert message.body == b"small"
            assert broker.connection_info.connection_id == 1
