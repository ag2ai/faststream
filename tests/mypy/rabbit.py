from collections.abc import Awaitable, Callable

import prometheus_client
from aio_pika import IncomingMessage
from aiormq.abc import ConfirmationFrameType
from typing_extensions import assert_type

from faststream._internal.basic_types import DecodedMessage
from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitRoute, RabbitRouter
from faststream.rabbit.fastapi import RabbitRouter as FastAPIRouter
from faststream.rabbit.opentelemetry import RabbitTelemetryMiddleware
from faststream.rabbit.prometheus import RabbitPrometheusMiddleware
from faststream.rabbit.publisher.usecase import RabbitPublisher
from faststream.rabbit.subscriber.usecase import RabbitSubscriber


def sync_decoder(msg: RabbitMessage) -> DecodedMessage:
    return ""


async def async_decoder(msg: RabbitMessage) -> DecodedMessage:
    return ""


async def custom_decoder(
    msg: RabbitMessage,
    original: Callable[[RabbitMessage], Awaitable[DecodedMessage]],
) -> DecodedMessage:
    return await original(msg)


RabbitBroker(decoder=sync_decoder)
RabbitBroker(decoder=async_decoder)
RabbitBroker(decoder=custom_decoder)


def sync_parser(msg: IncomingMessage) -> RabbitMessage:
    return ""  # type: ignore[return-value]


async def async_parser(msg: IncomingMessage) -> RabbitMessage:
    return ""  # type: ignore[return-value]


async def custom_parser(
    msg: IncomingMessage,
    original: Callable[[IncomingMessage], Awaitable[RabbitMessage]],
) -> RabbitMessage:
    return await original(msg)


RabbitBroker(parser=sync_parser)
RabbitBroker(parser=async_parser)
RabbitBroker(parser=custom_parser)


def sync_filter(msg: RabbitMessage) -> bool:
    return True


async def async_filter(msg: RabbitMessage) -> bool:
    return True


broker = RabbitBroker()

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


RabbitRouter(
    parser=sync_parser,
    decoder=sync_decoder,
)
RabbitRouter(
    parser=async_parser,
    decoder=async_decoder,
)
RabbitRouter(
    parser=custom_parser,
    decoder=custom_decoder,
)

router = RabbitRouter()

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


RabbitRouter(
    handlers=(
        RabbitRoute(sync_handler, "test"),
        RabbitRoute(async_handler, "test"),
        RabbitRoute(
            sync_handler,
            "test",
            parser=sync_parser,
            decoder=sync_decoder,
        ),
        RabbitRoute(
            sync_handler,
            "test",
            parser=async_parser,
            decoder=async_decoder,
        ),
        RabbitRoute(
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


otlp_middleware = RabbitTelemetryMiddleware()
RabbitBroker().add_middleware(otlp_middleware)
RabbitBroker(middlewares=[otlp_middleware])


prometheus_middleware = RabbitPrometheusMiddleware(registry=prometheus_client.REGISTRY)
RabbitBroker().add_middleware(prometheus_middleware)
RabbitBroker(middlewares=[prometheus_middleware])


async def check_publish_result_type() -> None:
    broker = RabbitBroker()

    publish_with_confirm = await broker.publish(None)
    assert_type(publish_with_confirm, ConfirmationFrameType | None)

    publisher = broker.publisher(queue="test")
    publish_with_confirm = await publisher.publish(None)

    assert_type(publish_with_confirm, ConfirmationFrameType | None)


async def check_request_response_type() -> None:
    broker = RabbitBroker()

    broker_response = await broker.request(None, "test")
    assert_type(broker_response, RabbitMessage)

    publisher = broker.publisher("test")
    publisher_response = await publisher.request(None, "test")
    assert_type(publisher_response, RabbitMessage)


async def check_subscriber_get_one_type(
    broker: RabbitBroker | FastAPIRouter | RabbitRouter,
) -> None:
    subscriber = broker.subscriber(queue="test")

    message = await subscriber.get_one()
    assert_type(message, RabbitMessage | None)

    async for msg in subscriber:
        assert_type(msg, RabbitMessage)


async def check_instance_type(
    broker: RabbitBroker | FastAPIRouter | RabbitRouter,
) -> None:
    subscriber = broker.subscriber(queue="test")
    assert_type(subscriber, RabbitSubscriber)

    publisher = broker.publisher(queue="test")
    assert_type(publisher, RabbitPublisher)


RabbitBroker(routers=[RabbitRouter()])
RabbitBroker().include_router(RabbitRouter())
RabbitBroker().include_routers(RabbitRouter())

RabbitRouter(routers=[RabbitRouter()])
RabbitRouter().include_router(RabbitRouter())
RabbitRouter().include_routers(RabbitRouter())
