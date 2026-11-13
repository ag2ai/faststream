from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from faststream import AckPolicy
from faststream.confluent import KafkaBroker, KafkaRouter
from faststream.confluent.configs import KafkaBrokerConfig
from faststream.confluent.helpers.client import AsyncConfluentConsumer
from faststream.confluent.helpers.config import ConfluentConfig, ConfluentFastConfig
from faststream.confluent.subscriber.config import KafkaSubscriberConfig
from faststream.exceptions import SetupError


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


def build_consumer(
    *,
    enable_auto_commit: bool,
    config: ConfluentConfig | None = None,
) -> AsyncConfluentConsumer:
    with patch("faststream.confluent.helpers.client.Consumer"):
        return AsyncConfluentConsumer(
            config=ConfluentFastConfig(config=config),
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
def test_manual_ack_rejects_auto_offset_store_override() -> None:
    with pytest.raises(SetupError, match=r"enable\.auto\.offset\.store"):
        build_consumer(
            enable_auto_commit=False,
            config=cast("ConfluentConfig", {"enable.auto.offset.store": True}),
        )


@pytest.mark.confluent()
def test_manual_ack_allows_disabled_auto_offset_store_override() -> None:
    consumer = build_consumer(
        enable_auto_commit=False,
        config=cast("ConfluentConfig", {"enable.auto.offset.store": False}),
    )

    assert consumer.config["enable.auto.offset.store"] is False


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_store_offset_delegates_to_native_consumer() -> None:
    consumer = build_consumer(enable_auto_commit=False)

    store_offsets = cast("MagicMock", consumer.consumer.store_offsets)

    await consumer.store_offset("topic", 2, 7)

    store_offsets.assert_called_once()
    (offsets,) = store_offsets.call_args.kwargs["offsets"]
    assert offsets.topic == "topic"
    assert offsets.partition == 2
    assert offsets.offset == 7
