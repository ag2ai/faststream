import asyncio
from types import MappingProxyType
from typing import TYPE_CHECKING

from confluent_kafka import Message
from typing_extensions import assert_type

from faststream.confluent import (
    ConfluentConfig,
    KafkaBroker,
    KafkaMessage,
    KafkaPublishMessage,
    KafkaPublisher,
    KafkaRouter,
    TestKafkaBroker,
)
from faststream.confluent.call_wrapper import KafkaHandlerCallWrapper
from faststream.confluent.fastapi import KafkaRouter as FastAPIRouter
from faststream.confluent.publisher.usecase import (
    BatchPublisher,
    DefaultPublisher,
)
from faststream.confluent.subscriber.usecase import (
    BatchSubscriber,
    DefaultSubscriber,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


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

    publish_default = await broker.publish(None, "test")
    assert_type(publish_default, Message | None)

    publish_without_confirm = await broker.publish(None, "test", no_confirm=True)
    assert_type(await publish_without_confirm, Message | None)

    publish_with_confirm = await broker.publish(None, "test", no_confirm=False)
    assert_type(publish_with_confirm, Message | None)

    publish_confirm_bool = await broker.publish(None, topic="test", no_confirm=fake_bool)
    assert_type(publish_confirm_bool, Message | asyncio.Future[Message | None] | None)


async def check_publish_headers() -> None:
    broker = KafkaBroker()
    text_headers: dict[str, str] = {"text": "value"}
    binary_headers: dict[str, bytes] = {"binary": b"\xff\x00"}
    headers: Mapping[str, str | bytes | None] = MappingProxyType({
        "text": "value",
        "binary": b"\xff\x00",
        "null": None,
    })

    await broker.publish("body", "test", headers=text_headers)
    await broker.publish("body", "test", headers=binary_headers)
    await broker.publish("body", "test", headers=headers)
    await broker.publish_batch("one", "two", topic="test", headers=headers)
    await broker.request("body", "test", headers=headers)
    KafkaPublishMessage("body", headers=binary_headers)


async def check_publisher_headers(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter,
) -> None:
    headers: Mapping[str, str | bytes | None] = MappingProxyType({
        "binary": b"\xff\x00",
        "null": None,
    })
    publisher = broker.publisher("test", headers=headers)
    await publisher.publish("body", headers=headers)
    await publisher.request("body", headers=headers)
    await broker.publisher("test", batch=True, headers=headers).publish(
        "one", "two", headers=headers
    )
    KafkaPublisher("test", headers=headers)


def check_raw_header_types(message: KafkaMessage) -> None:
    assert_type(message.raw_headers, dict[str, bytes | None])
    assert_type(message.batch_raw_headers, list[dict[str, bytes | None]])


async def check_publish_batch_per_message_attributes() -> None:
    broker = KafkaBroker()

    await broker.publish_batch(
        KafkaPublishMessage("user:1", key=b"user1"),
        "user:2",
        topic="test",
    )

    publisher = broker.publisher("test", batch=True)
    await publisher.publish(KafkaPublishMessage("user:1", key=b"user1"), "user:2")


async def check_publisher_publish_type(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter, fake_bool: bool = False
) -> None:
    p1 = broker.publisher("test", batch=False)
    assert_type(p1, DefaultPublisher)

    publish_default = await p1.publish(None, "test")
    assert_type(publish_default, Message | None)

    publish_without_confirm = await p1.publish(None, "test", no_confirm=True)
    assert_type(await publish_without_confirm, Message | None)

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
    assert_type(sub3, DefaultSubscriber)


async def check_call_assertions_take_the_kafka_fields(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter,
) -> None:
    # A sync handler: mypy and pyright spell an `async def`'s return type differently
    @broker.subscriber("test")
    def handle() -> None: ...

    assert_type(handle, KafkaHandlerCallWrapper[[], None])
    await handle.assert_called_once_with(None, key=b"k", partition=0)
    await handle.assert_called_with(key=b"k")
    await handle.assert_any_call(partition=0)

    # The publisher's own type is pinned in `check_publisher_instance_type`; here its
    # methods take the two fields, which only the Kafka mixin gives them
    publisher = broker.publisher("test")

    @publisher
    def published() -> None: ...

    assert_type(published, KafkaHandlerCallWrapper[[], None])
    await publisher.assert_called_once_with(None, key=b"k", partition=0)
    await publisher.assert_called_with(key=b"k")
    await publisher.assert_any_call(partition=0)


config: ConfluentConfig = {"topic.metadata.refresh.fast.interval.ms": 300}
KafkaBroker(config=config)

KafkaBroker(routers=[KafkaRouter()])
KafkaBroker().include_router(KafkaRouter())
KafkaBroker().include_routers(KafkaRouter())

KafkaRouter(routers=[KafkaRouter()])
KafkaRouter().include_router(KafkaRouter())
KafkaRouter().include_routers(KafkaRouter())

FastAPIRouter().include_router(KafkaRouter())


@KafkaBroker().subscriber("mykey", group_id="my_group", batch=True)
async def process_msgs() -> None:
    pass
