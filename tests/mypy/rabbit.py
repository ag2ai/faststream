from collections.abc import Awaitable
from typing import Callable

import prometheus_client
from aio_pika import IncomingMessage

from faststream._internal.basic_types import DecodedMessage
from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitRoute, RabbitRouter
from faststream.rabbit.fastapi import RabbitRouter as FastAPIRouter
from faststream.rabbit.opentelemetry import RabbitTelemetryMiddleware
from faststream.rabbit.prometheus import RabbitPrometheusMiddleware


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


def async_handler() -> None: ...


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
