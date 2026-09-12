from typing_extensions import assert_type

from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
from faststream.mqtt import (
    MQTTBroker,
    MQTTRouter,
    QoS,
    TestMQTTBroker,
    Will,
    WillProperties,
)
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


def check_decorated_handler_type(broker: MQTTBroker | MQTTRouter) -> None:
    # A union-typed `subscriber()` counts as an untyped decorator under strict mypy;
    # a sync handler, since mypy and pyright spell an `async def`'s return differently
    @broker.subscriber("test")
    def handle() -> None: ...

    assert_type(handle, HandlerCallWrapper[[], None])
