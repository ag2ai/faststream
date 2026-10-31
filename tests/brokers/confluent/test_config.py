from unittest.mock import MagicMock, patch

import pytest

from faststream import AckPolicy
from faststream.confluent import KafkaBroker, KafkaRouter
from faststream.confluent.configs import KafkaBrokerConfig
from faststream.confluent.helpers.client import AsyncConfluentConsumer
from faststream.confluent.helpers.config import ConfluentFastConfig
from faststream.confluent.subscriber.config import KafkaSubscriberConfig


@pytest.mark.confluent()
def test_default() -> None:
    config = KafkaSubscriberConfig(_outer_config=KafkaBrokerConfig())

    assert config.auto_ack_disabled
    assert config.ack_policy is AckPolicy.ACK_FIRST
    assert config.connection_data == {"enable_auto_commit": True}


@pytest.mark.confluent()
def test_ack_first() -> None:
    config = KafkaSubscriberConfig(
        _outer_config=MagicMock(),
        _ack_policy=AckPolicy.ACK_FIRST,
    )

    assert config.auto_ack_disabled
    assert config.ack_policy is AckPolicy.ACK_FIRST
    assert config.connection_data == {"enable_auto_commit": True}


@pytest.mark.confluent()
def test_custom_ack() -> None:
    config = KafkaSubscriberConfig(
        _outer_config=MagicMock(),
        _ack_policy=AckPolicy.REJECT_ON_ERROR,
    )

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR
    assert config.connection_data == {"enable_auto_commit": False}


@pytest.mark.confluent()
def test_broker_ack_policy() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = broker.subscriber("test", group_id="g")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.confluent()
def test_router_ack_policy() -> None:
    router = KafkaRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = router.subscriber("test", group_id="g")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.confluent()
@pytest.mark.rabbit()
def test_broker_ack_policy_without_router() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    router = KafkaRouter()
    broker.include_router(router)
    sub = router.subscriber("test", group_id="g")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.confluent()
def test_router_overrides_broker() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.ACK)
    router = KafkaRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber("test", group_id="g")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.confluent()
def test_sub_overrides_broker() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = broker.subscriber("test", group_id="g", ack_policy=AckPolicy.ACK)
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.confluent()
def test_sub_overrides_router() -> None:
    router = KafkaRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = router.subscriber("test", group_id="g", ack_policy=AckPolicy.ACK)
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.confluent()
def test_sub_overrides_broker_and_router() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    router = KafkaRouter(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber("test", group_id="g", ack_policy=AckPolicy.ACK)
    assert sub.ack_policy is AckPolicy.ACK


def build_consumer(*, enable_auto_commit: bool) -> AsyncConfluentConsumer:
    with patch("faststream.confluent.helpers.client.Consumer"):
        return AsyncConfluentConsumer(
            config=ConfluentFastConfig(),
            logger=MagicMock(),
            admin_service=MagicMock(),
            partitions=(),
            enable_auto_commit=enable_auto_commit,
        )


@pytest.mark.confluent()
@pytest.mark.parametrize(
    ("enable_auto_commit", "expected"),
    (
        pytest.param(True, True, id="ack-first"),
        pytest.param(False, False, id="manual-ack"),
    ),
)
def test_auto_offset_store_follows_auto_commit(
    enable_auto_commit: bool,
    expected: bool,
) -> None:
    consumer = build_consumer(enable_auto_commit=enable_auto_commit)

    assert consumer.config["enable.auto.commit"] is expected
    assert consumer.config["enable.auto.offset.store"] is expected


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_store_offsets_delegates_to_native_consumer() -> None:
    consumer = build_consumer(enable_auto_commit=False)

    await consumer.store_offsets("topic", 2, 7)

    consumer.consumer.store_offsets.assert_called_once()
    (offsets,) = consumer.consumer.store_offsets.call_args.kwargs["offsets"]
    assert offsets.topic == "topic"
    assert offsets.partition == 2
    assert offsets.offset == 7
