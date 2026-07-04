from collections.abc import Awaitable, Callable

import prometheus_client
from typing_extensions import assert_type

from faststream._internal.basic_types import DecodedMessage
from faststream.sqs import (
    SQSBroker,
    SQSMessage,
    SQSRoute,
    SQSRouter,
    TestSQSBroker,
)
from faststream.sqs.fastapi import SQSRouter as FastAPIRouter
from faststream.sqs.message import SQSRawMessage
from faststream.sqs.opentelemetry import SQSTelemetryMiddleware
from faststream.sqs.prometheus import SQSPrometheusMiddleware
from faststream.sqs.publisher.usecase import SQSBatchPublisher, SQSDefaultPublisher
from faststream.sqs.subscriber.usecase import ConcurrentSQSSubscriber, SQSSubscriber


async def check_multiple_test_brokers() -> None:
    async with TestSQSBroker(SQSBroker()) as br1:
        assert_type(br1, SQSBroker)
        await br1.publish(None, "test")

    async with TestSQSBroker(
        SQSBroker(),
        SQSBroker(),
    ) as (br1, br2):
        await br1.publish(None, "test")
        await br2.publish(None, "test")


def sync_decoder(msg: SQSMessage) -> DecodedMessage:
    return ""


async def async_decoder(msg: SQSMessage) -> DecodedMessage:
    return ""


async def custom_decoder(
    msg: SQSMessage,
    original: Callable[[SQSMessage], Awaitable[DecodedMessage]],
) -> DecodedMessage:
    return await original(msg)


SQSBroker(decoder=sync_decoder)
SQSBroker(decoder=async_decoder)
SQSBroker(decoder=custom_decoder)


def sync_parser(msg: SQSRawMessage) -> SQSMessage:
    return ""  # type: ignore[return-value]


async def async_parser(msg: SQSRawMessage) -> SQSMessage:
    return ""  # type: ignore[return-value]


async def custom_parser(
    msg: SQSRawMessage,
    original: Callable[[SQSRawMessage], Awaitable[SQSMessage]],
) -> SQSMessage:
    return await original(msg)


SQSBroker(parser=sync_parser)
SQSBroker(parser=async_parser)
SQSBroker(parser=custom_parser)


def sync_filter(msg: SQSMessage) -> bool:
    return True


async def async_filter(msg: SQSMessage) -> bool:
    return True


broker = SQSBroker()


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


SQSRouter(
    parser=sync_parser,
    decoder=sync_decoder,
)
SQSRouter(
    parser=async_parser,
    decoder=async_decoder,
)
SQSRouter(
    parser=custom_parser,
    decoder=custom_decoder,
)

router = SQSRouter()

router_sub = router.subscriber("test")


@router_sub(
    filter=sync_filter,
)
async def handle8() -> None: ...


@router_sub(
    filter=async_filter,
)
async def handle9() -> None: ...


@router.subscriber("test")
@router.publisher("test2")
def handle10() -> None: ...


@router.subscriber("test")
@router.publisher("test2")
async def handle11() -> None: ...


def sync_handler() -> None: ...


async def async_handler() -> None: ...


SQSRouter(
    handlers=(
        SQSRoute(sync_handler, "test"),
        SQSRoute(async_handler, "test"),
        SQSRoute(
            sync_handler,
            "test",
            parser=sync_parser,
            decoder=sync_decoder,
        ),
        SQSRoute(
            sync_handler,
            "test",
            parser=async_parser,
            decoder=async_decoder,
        ),
        SQSRoute(
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
async def handle12() -> None: ...


@fastapi_router.subscriber("test")
@fastapi_router.publisher("test2")
async def handle13() -> None: ...


otlp_middleware = SQSTelemetryMiddleware()
SQSBroker().add_middleware(otlp_middleware)
SQSBroker(middlewares=[otlp_middleware])


prometheus_middleware = SQSPrometheusMiddleware(registry=prometheus_client.REGISTRY)
SQSBroker().add_middleware(prometheus_middleware)
SQSBroker(middlewares=[prometheus_middleware])


async def check_request_response_type() -> None:
    broker = SQSBroker()

    broker_response = await broker.request(None, "test")
    assert_type(broker_response, SQSMessage)

    publisher = broker.publisher("test")
    publisher_response = await publisher.request(None, "test")
    assert_type(publisher_response, SQSMessage)


async def check_subscriber_message_type(
    broker: SQSBroker | SQSRouter,
) -> None:
    subscriber = broker.subscriber("test")

    message = await subscriber.get_one()
    assert_type(message, SQSMessage | None)

    async for msg in subscriber:
        assert_type(msg, SQSMessage)


def check_subscriber_instance_type(
    broker: SQSBroker | SQSRouter,
) -> None:
    sub1 = broker.subscriber("test")
    assert_type(sub1, SQSSubscriber)

    sub2 = broker.subscriber("test", max_workers=2)
    concurrent: ConcurrentSQSSubscriber | SQSSubscriber = sub2
    assert isinstance(concurrent, SQSSubscriber)


def check_publisher_instance_type(
    broker: SQSBroker | SQSRouter,
) -> None:
    pub1 = broker.publisher("test")
    assert_type(pub1, SQSDefaultPublisher)

    pub2 = broker.publisher("test", batch=True)
    assert_type(pub2, SQSBatchPublisher)


async def check_publish_types() -> None:
    broker = SQSBroker()

    await broker.publish("body", "queue")
    await broker.publish(b"body", "queue", group_id="g", deduplication_id="d")
    await broker.publish_batch("a", "b", queue="queue")

    publisher = broker.publisher("queue", batch=True)
    await publisher.publish(1, 2, 3)
    await publisher.publish(1, 2, 3, queue="other", group_id="g")


SQSBroker(routers=[SQSRouter()])
SQSBroker().include_router(SQSRouter())
SQSBroker().include_routers(SQSRouter())

SQSRouter(routers=[SQSRouter()])
SQSRouter().include_router(SQSRouter())
SQSRouter().include_routers(SQSRouter())
