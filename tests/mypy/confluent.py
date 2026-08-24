import asyncio

from confluent_kafka import Message
from typing_extensions import assert_type

from faststream import Config
from faststream.confluent import (
    KafkaBroker,
    KafkaMessage,
    KafkaPublisher,
    KafkaRoute,
    KafkaRouter,
    TestKafkaBroker,
)
from faststream.confluent.fastapi import KafkaRouter as FastAPIRouter
from faststream.confluent.publisher.usecase import (
    BatchPublisher,
    DefaultPublisher,
)
from faststream.confluent.subscriber.usecase import (
    BatchSubscriber,
    ConcurrentDefaultSubscriber,
    DefaultSubscriber,
)


async def check_multiple_test_brokers() -> None:
    async with TestKafkaBroker(KafkaBroker()) as br1:
        await br1.publish(None, "test")

    async with TestKafkaBroker(
        KafkaBroker(),
        KafkaBroker(),
    ) as (br1, br2):
        await br1.publish(None, "test")
        await br2.publish(None, "test")


async def check_response_type() -> None:
    broker = KafkaBroker()

    broker_response = await broker.request(None, "test")
    assert_type(broker_response, KafkaMessage)

    publisher = broker.publisher("test")
    assert_type(
        await publisher.request(
            None,
        ),
        KafkaMessage,
    )


async def check_publish_type(fake_bool: bool = True) -> None:
    broker = KafkaBroker()

    publish_with_confirm = await broker.publish(None, "test", no_confirm=True)
    assert_type(publish_with_confirm, asyncio.Future[Message | None])

    publish_without_confirm = await broker.publish(None, "test", no_confirm=False)
    assert_type(publish_without_confirm, Message | None)

    publish_confirm_bool = await broker.publish(None, topic="test", no_confirm=fake_bool)
    assert_type(publish_confirm_bool, Message | asyncio.Future[Message | None] | None)


async def check_publisher_publish_type(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter, fake_bool: bool = False
) -> None:
    p1 = broker.publisher("test", batch=False)
    assert_type(p1, DefaultPublisher)

    publish_without_confirm = await p1.publish(None, "test", no_confirm=True)
    assert_type(publish_without_confirm, asyncio.Future[Message | None])

    publish_with_confirm = await p1.publish(None, "test", no_confirm=False)
    assert_type(publish_with_confirm, Message | None)

    publish_confirm_bool = await p1.publish(None, "test", no_confirm=fake_bool)
    assert_type(publish_confirm_bool, Message | asyncio.Future[Message | None] | None)

    p2 = broker.publisher("test", batch=True)
    assert_type(p2, BatchPublisher)
    assert_type(await p2.publish(None, "test"), None)

    p3 = broker.publisher("test", batch=fake_bool)
    assert_type(p3, BatchPublisher | DefaultPublisher)


async def check_publish_batch_type(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter, fake_bool: bool = True
) -> None:
    broker = KafkaBroker()

    assert_type(
        await broker.publish_batch(None, topic="test"),
        None,
    )

    assert_type(
        await broker.publish_batch(None, topic="test", no_confirm=True),
        None,
    )

    assert_type(
        await broker.publish_batch(None, topic="test", no_confirm=fake_bool),
        None,
    )


async def check_channel_subscriber(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter,
) -> None:
    subscriber = broker.subscriber("test")

    message = await subscriber.get_one()
    assert_type(message, KafkaMessage | None)

    async for msg in subscriber:
        assert_type(msg, KafkaMessage)


def check_subscriber_instance_type(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter,
) -> None:
    sub1 = broker.subscriber("test")
    assert_type(sub1, DefaultSubscriber)

    sub2 = broker.subscriber("test", batch=True)
    assert_type(sub2, BatchSubscriber)

    sub3 = broker.subscriber("test", max_workers=2)
    assert_type(sub3, ConcurrentDefaultSubscriber)


KafkaBroker(routers=[KafkaRouter()])
KafkaBroker().include_router(KafkaRouter())
KafkaBroker().include_routers(KafkaRouter())

KafkaRouter(routers=[KafkaRouter()])
KafkaRouter().include_router(KafkaRouter())
KafkaRouter().include_routers(KafkaRouter())


@KafkaBroker().subscriber("mykey", group_id="my_group", batch=True)
async def process_msgs() -> None:
    pass


# --- Config placeholders ------------------------------------------------------
#
# A placeholder is accepted on address parameters and nowhere else, and that
# boundary is the signature alone — there is no runtime guard (ADR-0002). The
# `type: ignore` comments below are therefore assertions, not suppressions:
# `warn_unused_ignores` is on, so if any of these positions ever starts
# type-checking, the ignore goes unused and the build fails.
#
# Confluent has no `pattern` parameter — it subscribes by topic only — so the
# allowlist here is `topics`, `group_id`, `topic` and `reply_to`.


async def async_handler() -> None: ...


def check_config_on_subscriber_address_params(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter,
) -> None:
    broker.subscriber(Config("TOPIC"))
    broker.subscriber(Config("TOPIC"), Config("TOPIC2"))
    broker.subscriber(Config("TOPIC"), "literal-topic")
    broker.subscriber("test", group_id=Config("GROUP"))
    broker.subscriber(Config("TOPIC"), group_id=Config("GROUP"))
    broker.subscriber("test", group_id=Config("GROUP"), max_workers=2)
    broker.subscriber(Config("TOPIC"), batch=True)


def check_config_on_publisher_address_params(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter,
) -> None:
    broker.publisher(Config("TOPIC"))
    broker.publisher("test", reply_to=Config("REPLY"))
    broker.publisher(Config("TOPIC"), reply_to=Config("REPLY"))
    broker.publisher(Config("TOPIC"), batch=True)


def check_config_on_router_containers() -> None:
    KafkaRouter(
        handlers=(
            KafkaRoute(async_handler, Config("TOPIC")),
            KafkaRoute(async_handler, "test", group_id=Config("GROUP")),
            KafkaRoute(
                async_handler,
                "test",
                publishers=(
                    KafkaPublisher(Config("TOPIC")),
                    KafkaPublisher("test", reply_to=Config("REPLY")),
                ),
            ),
        ),
    )


def check_config_publisher_instance_type(broker: KafkaBroker) -> None:
    assert_type(broker.publisher(Config("TOPIC")), DefaultPublisher)
    assert_type(broker.publisher(Config("TOPIC"), batch=True), BatchPublisher)


async def check_config_is_rejected_by_runtime_publishing() -> None:
    broker = KafkaBroker()

    await broker.publish(None, Config("TOPIC"))  # type: ignore[call-overload]
    await broker.publish(None, "test", reply_to=Config("REPLY"))  # type: ignore[call-overload]
    await broker.publish_batch(None, topic=Config("TOPIC"))  # type: ignore[arg-type]
    await broker.request(None, Config("TOPIC"))  # type: ignore[arg-type]

    publisher = broker.publisher("test")
    await publisher.publish(None, Config("TOPIC"))  # type: ignore[call-overload]
    await publisher.publish(None, "test", reply_to=Config("REPLY"))  # type: ignore[call-overload]
    await publisher.request(None, Config("TOPIC"))  # type: ignore[arg-type]

    batch_publisher = broker.publisher("test", batch=True)
    await batch_publisher.publish(None, topic=Config("TOPIC"))  # type: ignore[arg-type]


def check_config_is_rejected_on_structural_params(broker: KafkaBroker) -> None:
    broker.subscriber("test", batch=Config("BATCH"))  # type: ignore[call-overload]
    broker.subscriber("test", max_workers=Config("WORKERS"))  # type: ignore[call-overload]
    broker.subscriber("test", ack_policy=Config("ACK"))  # type: ignore[call-overload]
    broker.subscriber("test", no_reply=Config("NO_REPLY"))  # type: ignore[call-overload]
    broker.subscriber("test", polling_interval=Config("INTERVAL"))  # type: ignore[call-overload]
    broker.publisher("test", batch=Config("BATCH"))  # type: ignore[call-overload]
    broker.publisher("test", partition=Config("PARTITION"))  # type: ignore[call-overload]


def check_config_is_rejected_on_the_confluent_config(broker: KafkaBroker) -> None:
    """`config` is librdkafka's, not FastStream's — no placeholder there."""
    KafkaBroker(config=Config("CLIENT_CONFIG"))  # type: ignore[arg-type]
