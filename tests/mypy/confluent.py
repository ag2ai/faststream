import asyncio

from confluent_kafka import Message
from typing_extensions import assert_type

from faststream.confluent import KafkaBroker, KafkaMessage
from faststream.confluent.publisher.usecase import (
    BatchPublisher,
    DefaultPublisher,
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


async def check_publish_type() -> None:
    broker = KafkaBroker()

    publish_with_confirm = await broker.publish(None, "test", no_confirm=True)
    assert_type(publish_with_confirm, asyncio.Future[Message | None])

    publish_without_confirm = await broker.publish(None, "test", no_confirm=False)
    assert_type(publish_without_confirm, Message | None)


async def check_publisher_publish_type(fake_bool: bool = False) -> None:
    broker = KafkaBroker()

    p1 = broker.publisher("test", batch=True)
    assert_type(p1, BatchPublisher)

    publish_with_confirm = await p1.publish(None, "test")  # type: ignore[func-returns-value]
    assert_type(publish_with_confirm, None)

    p2 = broker.publisher("test", batch=False)
    assert_type(p2, DefaultPublisher)

    publish_without_confirm = await p2.publish(None, "test", no_confirm=True)
    assert_type(publish_without_confirm, asyncio.Future[Message | None])

    publish_with_confirm = await p2.publish(None, "test", no_confirm=False)
    assert_type(publish_with_confirm, Message | None)

    p3 = broker.publisher("test", batch=fake_bool)
    assert_type(p3, BatchPublisher | DefaultPublisher)


async def check_publish_batch_type() -> None:
    broker = KafkaBroker()

    publish_with_confirm = await broker.publish_batch(None, topic="test")  # type: ignore[func-returns-value]
    assert_type(publish_with_confirm, None)


async def check_channel_subscriber() -> None:
    broker = KafkaBroker()

    subscriber = broker.subscriber("test")

    message = await subscriber.get_one()
    assert_type(message, KafkaMessage | None)

    async for msg in subscriber:
        assert_type(msg, KafkaMessage)
