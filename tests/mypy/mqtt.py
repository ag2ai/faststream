from typing_extensions import assert_type

from faststream.mqtt import (
    MQTTBroker,
    MQTTRouter,
    QoS,
    TestMQTTBroker,
    Will,
    WillProperties,
)
from faststream.mqtt.call_wrapper import MqttHandlerCallWrapper
from faststream.mqtt.subscriber.usecase import (
    MQTTConcurrentSubscriber,
    MQTTDefaultSubscriber,
)


async def on_connection_recovery_failed() -> None:
    pass


MQTTBroker("mqtts://localhost")
MQTTBroker(url="mqtt://localhost:1884")
MQTTBroker(host="localhost", port=1884)
MQTTBroker(
    version="5.0",
    will=Will(
        topic="status/service",
        payload=b"offline",
        qos=QoS.AT_LEAST_ONCE,
        retain=True,
        properties=WillProperties(content_type="text/plain"),
    ),
    on_connection_recovery_failed=on_connection_recovery_failed,
    session_replay_buffer_size=5000,
    session_replay_timeout=60.0,
)


async def check_multiple_test_brokers() -> None:
    async with TestMQTTBroker(MQTTBroker()) as br1:
        await br1.publish(None, "test")

    async with TestMQTTBroker(
        MQTTBroker(),
        MQTTBroker(),
    ) as (br1, br2):
        await br1.publish(None, "test")
        await br2.publish(None, "test")


def check_subscriber_instance_type(broker: MQTTBroker | MQTTRouter) -> None:
    sub1 = broker.subscriber("test")
    assert_type(sub1, MQTTDefaultSubscriber)

    sub2 = broker.subscriber("test", max_workers=2)
    assert_type(sub2, MQTTConcurrentSubscriber)


async def check_call_assertions_take_the_mqtt_fields(
    broker: MQTTBroker | MQTTRouter,
) -> None:
    # A sync handler: mypy and pyright spell an `async def`'s return type differently
    @broker.subscriber("test")
    def handle() -> None: ...

    assert_type(handle, MqttHandlerCallWrapper[[], None])
    await handle.assert_called_once_with(
        None,
        topic="test",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
    )
    await handle.assert_called_with(topic="test")
    await handle.assert_any_call(qos=QoS.AT_MOST_ONCE, retain=False)

    # The publisher's methods take the three fields, which only the MQTT mixin gives them
    publisher = broker.publisher("test")

    @publisher
    def published() -> None: ...

    assert_type(published, MqttHandlerCallWrapper[[], None])
    await publisher.assert_called_once_with(
        None,
        topic="test",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
    )
    await publisher.assert_called_with(topic="test")
    await publisher.assert_any_call(qos=QoS.AT_MOST_ONCE, retain=False)
