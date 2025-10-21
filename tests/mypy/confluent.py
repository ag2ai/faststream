import asyncio

from confluent_kafka import Message
from typing_extensions import assert_type

from faststream.confluent import KafkaBroker, KafkaMessage, KafkaRouter
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
