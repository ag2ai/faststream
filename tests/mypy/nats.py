from collections.abc import Awaitable
from typing import Callable

import prometheus_client
from nats.aio.msg import Msg

from faststream._internal.basic_types import DecodedMessage
from faststream.nats import NatsBroker, NatsMessage, NatsRoute, NatsRouter
from faststream.nats.fastapi import NatsRouter as FastAPIRouter
from faststream.nats.opentelemetry import NatsTelemetryMiddleware
from faststream.nats.prometheus import NatsPrometheusMiddleware


def sync_decoder(msg: NatsMessage) -> DecodedMessage:
    return ""


async def async_decoder(msg: NatsMessage) -> DecodedMessage:
    return ""


async def custom_decoder(
    msg: NatsMessage,
    original: Callable[[NatsMessage], Awaitable[DecodedMessage]],
) -> DecodedMessage:
    return await original(msg)


NatsBroker(decoder=sync_decoder)
NatsBroker(decoder=async_decoder)
NatsBroker(decoder=custom_decoder)


def sync_parser(msg: Msg) -> NatsMessage:
    return ""  # type: ignore[return-value]


async def async_parser(msg: Msg) -> NatsMessage:
    return ""  # type: ignore[return-value]


async def custom_parser(
    msg: Msg,
    original: Callable[[Msg], Awaitable[NatsMessage]],
) -> NatsMessage:
    return await original(msg)


NatsBroker(parser=sync_parser)
NatsBroker(parser=async_parser)
NatsBroker(parser=custom_parser)


def sync_filter(msg: NatsMessage) -> bool:
    return True


async def async_filter(msg: NatsMessage) -> bool:
    return True


broker = NatsBroker()


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


NatsRouter(
    parser=sync_parser,
    decoder=sync_decoder,
)
NatsRouter(
    parser=async_parser,
    decoder=async_decoder,
)
NatsRouter(
    parser=custom_parser,
    decoder=custom_decoder,
)

router = NatsRouter()


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


NatsRouter(
    handlers=(
        NatsRoute(sync_handler, "test"),
        NatsRoute(async_handler, "test"),
        NatsRoute(
            sync_handler,
            "test",
            parser=sync_parser,
            decoder=sync_decoder,
        ),
        NatsRoute(
            sync_handler,
            "test",
            parser=async_parser,
            decoder=async_decoder,
        ),
        NatsRoute(
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


otlp_middleware = NatsTelemetryMiddleware()
NatsBroker().add_middleware(otlp_middleware)
NatsBroker(middlewares=[otlp_middleware])


prometheus_middleware = NatsPrometheusMiddleware(registry=prometheus_client.REGISTRY)
NatsBroker().add_middleware(prometheus_middleware)
NatsBroker(middlewares=[prometheus_middleware])
