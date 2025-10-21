import asyncio
from collections.abc import Awaitable, Callable

import prometheus_client
from typing_extensions import assert_type

from faststream._internal.basic_types import DecodedMessage
from faststream.kafka import (
    ConsumerRecord,
    KafkaBroker,
    KafkaMessage,
    KafkaRoute,
    KafkaRouter,
    RecordMetadata,
)
from faststream.kafka.fastapi import KafkaRouter as FastAPIRouter
from faststream.kafka.opentelemetry import KafkaTelemetryMiddleware
from faststream.kafka.prometheus import KafkaPrometheusMiddleware
from faststream.kafka.publisher import BatchPublisher, DefaultPublisher
from faststream.kafka.subscriber.usecase import (
    BatchSubscriber,
    ConcurrentBetweenPartitionsSubscriber,
    ConcurrentDefaultSubscriber,
    DefaultSubscriber,
)


def sync_decoder(msg: KafkaMessage) -> DecodedMessage:
    return ""


async def async_decoder(msg: KafkaMessage) -> DecodedMessage:
    return ""


async def custom_decoder(
    msg: KafkaMessage,
    original: Callable[[KafkaMessage], Awaitable[DecodedMessage]],
) -> DecodedMessage:
    return await original(msg)


KafkaBroker(decoder=sync_decoder)
KafkaBroker(decoder=async_decoder)
KafkaBroker(decoder=custom_decoder)


def sync_parser(msg: ConsumerRecord) -> KafkaMessage:
    return ""  # type: ignore[return-value]


async def async_parser(msg: ConsumerRecord) -> KafkaMessage:
    return ""  # type: ignore[return-value]


async def custom_parser(
    msg: ConsumerRecord,
    original: Callable[[ConsumerRecord], Awaitable[KafkaMessage]],
) -> KafkaMessage:
    return await original(msg)


KafkaBroker(parser=sync_parser)
KafkaBroker(parser=async_parser)
KafkaBroker(parser=custom_parser)


def sync_filter(msg: KafkaMessage) -> bool:
    return True


async def async_filter(msg: KafkaMessage) -> bool:
    return True


broker = KafkaBroker()


sub = broker.subscriber("test")


@sub(
    filter=sync_filter,
)
async def handle() -> None: ...


@sub(
    filter=async_filter,
)
async def handle2() -> None: ...


@broker.subscriber(
    "test",
    parser=sync_parser,
    decoder=sync_decoder,
)
async def handle3() -> None: ...


@broker.subscriber(
    "test",
    parser=async_parser,
    decoder=async_decoder,
)
async def handle4() -> None: ...


@broker.subscriber(
    "test",
    parser=custom_parser,
    decoder=custom_decoder,
)
async def handle5() -> None: ...


@broker.subscriber("test")
@broker.publisher("test2")
def handle6() -> None: ...


@broker.subscriber("test")
@broker.publisher("test2")
async def handle7() -> None: ...


KafkaRouter(
    parser=sync_parser,
    decoder=sync_decoder,
)
KafkaRouter(
    parser=async_parser,
    decoder=async_decoder,
)
KafkaRouter(
    parser=custom_parser,
    decoder=custom_decoder,
)

router = KafkaRouter()

router_sub = router.subscriber("test")


@router_sub(
    filter=sync_filter,
)
async def handle8() -> None: ...


@router_sub(
    filter=async_filter,
)
async def handle9() -> None: ...


@router.subscriber(
    "test",
    parser=sync_parser,
    decoder=sync_decoder,
)
async def handle10() -> None: ...


@router.subscriber(
    "test",
    parser=async_parser,
    decoder=async_decoder,
)
async def handle11() -> None: ...


@router.subscriber(
    "test",
    parser=custom_parser,
    decoder=custom_decoder,
)
async def handle12() -> None: ...


@router.subscriber("test")
@router.publisher("test2")
def handle13() -> None: ...


@router.subscriber("test")
@router.publisher("test2")
async def handle14() -> None: ...


def sync_handler() -> None: ...


async def async_handler() -> None: ...


KafkaRouter(
    handlers=(
        KafkaRoute(sync_handler, "test"),
        KafkaRoute(async_handler, "test"),
        KafkaRoute(
            sync_handler,
            "test",
            parser=sync_parser,
            decoder=sync_decoder,
        ),
        KafkaRoute(
            sync_handler,
            "test",
            parser=async_parser,
            decoder=async_decoder,
        ),
        KafkaRoute(
            sync_handler,
            "test",
            parser=custom_parser,
            decoder=custom_decoder,
        ),
    ),
)


FastAPIRouter(
    parser=sync_parser,
    decoder=sync_decoder,
)
FastAPIRouter(
    parser=async_parser,
    decoder=async_decoder,
)
FastAPIRouter(
    parser=custom_parser,
    decoder=custom_decoder,
)

fastapi_router = FastAPIRouter()

fastapi_sub = fastapi_router.subscriber("test")


@fastapi_sub(
    filter=sync_filter,
)
async def handle15() -> None: ...


@fastapi_sub(
    filter=async_filter,
)
async def handle16() -> None: ...


@fastapi_router.subscriber(
    "test",
    parser=sync_parser,
    decoder=sync_decoder,
)
async def handle17() -> None: ...


@fastapi_router.subscriber(
    "test",
    parser=async_parser,
    decoder=async_decoder,
)
async def handle18() -> None: ...


@fastapi_router.subscriber(
    "test",
    parser=custom_parser,
    decoder=custom_decoder,
)
async def handle19() -> None: ...


@fastapi_router.subscriber("test")
@fastapi_router.publisher("test2")
def handle20() -> None: ...


@fastapi_router.subscriber("test")
@fastapi_router.publisher("test2")
async def handle21() -> None: ...


otlp_middleware = KafkaTelemetryMiddleware()
KafkaBroker().add_middleware(otlp_middleware)
KafkaBroker(middlewares=[otlp_middleware])


prometheus_middleware = KafkaPrometheusMiddleware(registry=prometheus_client.REGISTRY)
KafkaBroker().add_middleware(prometheus_middleware)
KafkaBroker(middlewares=[prometheus_middleware])


async def check_broker_publish_result_type() -> None:
    broker = KafkaBroker()

    publish_with_confirm = await broker.publish(None, "test")
    assert_type(publish_with_confirm, RecordMetadata)

    publish_without_confirm = await broker.publish(None, "test", no_confirm=True)
    assert_type(await publish_without_confirm, RecordMetadata)

    publish_confirm_bool = await broker.publish(None, "test", no_confirm=fake_bool())
    assert_type(publish_confirm_bool, RecordMetadata | asyncio.Future[RecordMetadata])


async def check_publisher_publish_result_types() -> None:
    broker = KafkaBroker()

    publisher = broker.publisher("test")

    publish_with_confirm = await publisher.publish(None, "test")
    assert_type(publish_with_confirm, RecordMetadata)

    publish_without_confirm = await publisher.publish(None, "test", no_confirm=True)
    assert_type(await publish_without_confirm, RecordMetadata)

    publish_confirm_bool = await publisher.publish(None, "test", no_confirm=fake_bool())
    assert_type(publish_confirm_bool, RecordMetadata | asyncio.Future[RecordMetadata])


async def check_publish_batch_result_type() -> None:
    broker = KafkaBroker()

    publish_with_confirm = await broker.publish_batch(None, topic="test")
    assert_type(publish_with_confirm, RecordMetadata)

    publish_without_confirm = await broker.publish_batch(
        None, topic="test", no_confirm=True
    )
    assert_type(await publish_without_confirm, RecordMetadata)

    publish_confirm_bool = await broker.publish_batch(
        None, topic="test", no_confirm=fake_bool()
    )
    assert_type(publish_confirm_bool, RecordMetadata | asyncio.Future[RecordMetadata])


async def check_publisher_publish_batch_result_type() -> None:
    broker = KafkaBroker()

    publisher = broker.publisher("test", batch=True)

    publish_with_confirm = await publisher.publish(None, topic="test")
    assert_type(publish_with_confirm, RecordMetadata)

    publish_without_confirm = await publisher.publish(None, topic="test", no_confirm=True)
    assert_type(await publish_without_confirm, RecordMetadata)

    publish_confirm_bool = await publisher.publish(
        None, topic="test", no_confirm=fake_bool()
    )
    assert_type(publish_confirm_bool, RecordMetadata | asyncio.Future[RecordMetadata])


async def check_request_response_type() -> None:
    broker = KafkaBroker()

    broker_response = await broker.request(None, "test")
    assert_type(broker_response, KafkaMessage)

    publisher = broker.publisher("test")
    publisher_response = await publisher.request(None, "test")
    assert_type(publisher_response, KafkaMessage)


async def check_subscriber_message_type(
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

    sub4 = broker.subscriber("test", group_id="test", max_workers=2)
    assert_type(sub4, ConcurrentBetweenPartitionsSubscriber)


def check_publisher_instance_type(
    broker: KafkaBroker | FastAPIRouter | KafkaRouter,
) -> None:
    pub1 = broker.publisher("test")
    assert_type(pub1, DefaultPublisher)

    pub2 = broker.publisher("test", batch=True)
    assert_type(pub2, BatchPublisher)


def fake_bool() -> bool:
    return True


KafkaBroker(routers=[KafkaRouter()])
KafkaBroker().include_router(KafkaRouter())
KafkaBroker().include_routers(KafkaRouter())

KafkaRouter(routers=[KafkaRouter()])
KafkaRouter().include_router(KafkaRouter())
KafkaRouter().include_routers(KafkaRouter())
