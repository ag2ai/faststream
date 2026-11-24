from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from faststream import AckPolicy
from faststream.confluent import KafkaBroker, KafkaRouter
from faststream.confluent.configs import KafkaBrokerConfig
from faststream.confluent.helpers.client import AsyncConfluentConsumer
from faststream.confluent.helpers.config import ConfluentConfig, ConfluentFastConfig
from faststream.confluent.schemas import TopicPartition
from faststream.confluent.subscriber.config import KafkaSubscriberConfig
from faststream.exceptions import SetupError

# The precedence cases put REJECT_ON_ERROR on a broker or a router, where Kafka warns
pytestmark = pytest.mark.filterwarnings(
    "ignore:AckPolicy.REJECT_ON_ERROR has the same effect",
)


@pytest.mark.confluent()
def test_default() -> None:
    config = KafkaSubscriberConfig(_outer_config=KafkaBrokerConfig())

    assert config.auto_ack_disabled
    assert config.ack_policy is AckPolicy.ACK_FIRST


@pytest.mark.confluent()
def test_ack_first() -> None:
    config = KafkaSubscriberConfig(
        _outer_config=MagicMock(),
        _ack_policy=AckPolicy.ACK_FIRST,
    )

    assert config.auto_ack_disabled
    assert config.ack_policy is AckPolicy.ACK_FIRST


@pytest.mark.confluent()
def test_custom_ack() -> None:
    config = KafkaSubscriberConfig(
        _outer_config=MagicMock(),
        _ack_policy=AckPolicy.REJECT_ON_ERROR,
    )

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR


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


@pytest.mark.confluent()
def test_late_included_router_subscriber_disables_auto_commit() -> None:
    """Policy and auto-commit follow the broker as soon as the router is included."""
    router = KafkaRouter()
    subscriber: Any = router.subscriber("orders", group_id="workers")
    broker = KafkaBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)

    assert (
        subscriber.ack_policy,
        subscriber._connection_data["enable_auto_commit"],
    ) == (AckPolicy.NACK_ON_ERROR, False)


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
async def test_store_offsets_delegates_to_native_consumer() -> None:
    consumer = build_consumer(enable_auto_commit=False)

    store_offsets = cast("MagicMock", consumer.consumer.store_offsets)

    await consumer.store_offsets(
        TopicPartition("topic", 2, 7),
        TopicPartition("other", 0, 9),
    )

    store_offsets.assert_called_once()
    first, second = store_offsets.call_args.kwargs["offsets"]
    assert (first.topic, first.partition, first.offset) == ("topic", 2, 7)
    assert (second.topic, second.partition, second.offset) == ("other", 0, 9)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_store_offsets_with_no_partitions_does_not_call_native_consumer() -> None:
    consumer = build_consumer(enable_auto_commit=False)

    await consumer.store_offsets()

    cast("MagicMock", consumer.consumer.store_offsets).assert_not_called()
