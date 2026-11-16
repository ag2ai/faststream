from unittest.mock import AsyncMock, patch

import pytest

from faststream import AckPolicy
from faststream.confluent import (
    KafkaBroker as ConfluentBroker,
    KafkaRouter as ConfluentRouter,
)
from faststream.exceptions import SetupError
from faststream.kafka import KafkaBroker, KafkaRouter
from faststream.kafka.message import KafkaAckableMessage
from faststream.rabbit import RabbitBroker, RabbitRouter, TestRabbitBroker
from faststream.rabbit.message import RabbitMessage
from faststream.redis import RedisBroker, RedisRouter, StreamSub


def test_kafka_router_inherits_broker_policy_after_registration() -> None:
    router = KafkaRouter()
    subscriber = router.subscriber("orders", group_id="workers")
    broker = KafkaBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)

    assert subscriber.ack_policy is AckPolicy.NACK_ON_ERROR
    assert subscriber._connection_args["enable_auto_commit"] is False
    assert subscriber.parser.msg_class is KafkaAckableMessage


def test_rabbit_router_inherits_broker_policy_after_registration() -> None:
    router = RabbitRouter()
    subscriber = router.subscriber("orders")
    broker = RabbitBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)

    assert subscriber.ack_policy is AckPolicy.NACK_ON_ERROR


def test_redis_router_inherits_broker_policy_after_registration() -> None:
    router = RedisRouter()
    subscriber = router.subscriber(
        stream=StreamSub("orders", group="workers", consumer="worker")
    )
    broker = RedisBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)

    assert subscriber.ack_policy is AckPolicy.NACK_ON_ERROR


def test_confluent_router_inherits_broker_policy_after_registration() -> None:
    router = ConfluentRouter()
    subscriber = router.subscriber("orders", group_id="workers")
    broker = ConfluentBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)

    assert subscriber.ack_policy is AckPolicy.NACK_ON_ERROR
    assert subscriber.parser.is_manual


def test_nested_router_and_subscriber_overrides() -> None:
    inner = KafkaRouter()
    inherited = inner.subscriber("orders", group_id="workers")
    explicit = inner.subscriber(
        "payments", group_id="workers", ack_policy=AckPolicy.ACK_FIRST
    )
    outer = KafkaRouter()
    outer.include_router(inner)
    broker = KafkaBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(outer)

    assert inherited.ack_policy is AckPolicy.NACK_ON_ERROR
    assert inherited.parser.msg_class is KafkaAckableMessage
    assert explicit.ack_policy is AckPolicy.ACK_FIRST
    assert explicit._connection_args["enable_auto_commit"] is True


def test_router_reattachment_refreshes_policy_and_parser() -> None:
    router = KafkaRouter()
    subscriber = router.subscriber("orders", group_id="workers")
    first = KafkaBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    second = KafkaBroker(ack_policy=AckPolicy.ACK_FIRST)
    first.include_router(router)
    second.include_router(router)

    assert subscriber.ack_policy is AckPolicy.ACK_FIRST
    assert subscriber._connection_args["enable_auto_commit"] is True
    assert subscriber.parser.msg_class is not KafkaAckableMessage


@pytest.mark.asyncio()
async def test_kafka_late_policy_with_multiple_workers_is_rejected() -> None:
    router = KafkaRouter()
    subscriber = router.subscriber("orders", group_id="workers", max_workers=2)
    KafkaBroker(ack_policy=AckPolicy.NACK_ON_ERROR).include_router(router)

    with pytest.raises(SetupError, match="router acknowledgement policy"):
        await subscriber.start()


@pytest.mark.asyncio()
async def test_confluent_late_policy_with_multiple_workers_is_rejected() -> None:
    router = ConfluentRouter()
    subscriber = router.subscriber("orders", group_id="workers", max_workers=2)
    ConfluentBroker(ack_policy=AckPolicy.NACK_ON_ERROR).include_router(router)

    with pytest.raises(SetupError, match="manual commit"):
        await subscriber.start()


@pytest.mark.asyncio()
async def test_inherited_rabbit_policy_nacks_failed_handler() -> None:
    router = RabbitRouter()

    @router.subscriber("orders")
    async def handler(_: str) -> None:
        msg = "handler failed"
        raise ValueError(msg)

    broker = RabbitBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)
    with patch.object(RabbitMessage, "nack", new_callable=AsyncMock) as nack:
        async with TestRabbitBroker(broker) as test_broker:
            with pytest.raises(ValueError, match="handler failed"):
                await test_broker.publish("test", queue="orders")

    nack.assert_awaited_once()
