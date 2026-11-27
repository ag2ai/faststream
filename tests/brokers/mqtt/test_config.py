from collections.abc import Callable
from typing import Any

import pytest

from faststream import AckPolicy
from faststream.mqtt import MQTTBroker, MQTTRouter, QoS, Will


@pytest.mark.mqtt()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "options",
    (
        {"receive_maximum": 10},
        {"maximum_packet_size": 4096},
        {"user_properties": (("role", "worker"),)},
        {"request_response_information": False},
        {"request_problem_information": False},
    ),
)
async def test_broker_connect_properties_reject_v311(options: dict[str, Any]) -> None:
    broker = MQTTBroker(version="3.1.1", **options)
    with pytest.raises(RuntimeError, match=r"MQTT 5\.0 is required"):
        await broker.connect()


@pytest.mark.mqtt()
def test_default() -> None:
    broker = MQTTBroker()
    sub = broker.subscriber("test")
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.mqtt()
def test_broker_ack_policy() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = broker.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_router_ack_policy() -> None:
    router = MQTTRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_broker_ack_policy_without_router() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    router = MQTTRouter()
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_router_overrides_broker() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.ACK_FIRST)
    router = MQTTRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_sub_overrides_broker() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.ACK)
    sub = broker.subscriber("test", ack_policy=AckPolicy.REJECT_ON_ERROR)
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_sub_overrides_router() -> None:
    router = MQTTRouter(ack_policy=AckPolicy.ACK)
    sub = router.subscriber("test", ack_policy=AckPolicy.REJECT_ON_ERROR)
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
@pytest.mark.filterwarnings("ignore:MQTT has no nack primitive")
def test_sub_overrides_broker_and_router() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.ACK)

    router = MQTTRouter(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)

    sub = router.subscriber("test", ack_policy=AckPolicy.ACK_FIRST)
    assert sub.ack_policy is AckPolicy.ACK_FIRST


@pytest.mark.mqtt()
@pytest.mark.parametrize(
    "set_policy",
    (
        pytest.param(
            lambda: MQTTBroker(ack_policy=AckPolicy.NACK_ON_ERROR),
            id="broker",
        ),
        pytest.param(
            lambda: MQTTRouter(ack_policy=AckPolicy.NACK_ON_ERROR),
            id="router",
        ),
        pytest.param(
            lambda: MQTTBroker().subscriber("test", ack_policy=AckPolicy.NACK_ON_ERROR),
            id="subscriber",
        ),
    ),
)
def test_nack_on_error_warns_where_it_is_set(set_policy: Callable[[], object]) -> None:
    with pytest.warns(RuntimeWarning, match="MQTT has no nack primitive") as record:
        set_policy()

    # the warning points at the line that set the policy, whatever the level
    assert [w.filename for w in record if "no nack" in str(w.message)] == [__file__]


@pytest.mark.mqtt()
def test_mqtt_connect_timeout_threaded_to_client() -> None:
    broker = MQTTBroker(mqtt_connect_timeout=7.0)
    assert broker._connection_kwargs["mqtt_connect_timeout"] == 7


@pytest.mark.mqtt()
def test_recovery_callback_threaded_to_client() -> None:
    async def on_connection_recovery_failed() -> None:
        pass

    broker = MQTTBroker(
        on_connection_recovery_failed=on_connection_recovery_failed,
    )

    assert (
        broker._connection_kwargs["on_connection_recovery_failed"]
        is on_connection_recovery_failed
    )


@pytest.mark.mqtt()
def test_session_replay_config_threaded_to_client() -> None:
    broker = MQTTBroker(
        session_replay_buffer_size=10,
        session_replay_timeout=20.0,
    )

    assert broker._connection_kwargs["session_replay_buffer_size"] == 10
    assert broker._connection_kwargs["session_replay_timeout"] == 20


@pytest.mark.mqtt()
def test_will_threaded_to_client() -> None:
    will = Will(
        topic="status/service",
        payload=b"offline",
        qos=QoS.AT_LEAST_ONCE,
        retain=True,
    )

    broker = MQTTBroker(will=will)

    assert broker._connection_kwargs["will"] is will
