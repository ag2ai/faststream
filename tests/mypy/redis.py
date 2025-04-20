from collections.abc import Awaitable
from typing import Callable

import prometheus_client

from faststream._internal.basic_types import DecodedMessage
from faststream.redis import (
    RedisBroker as Broker,
    RedisMessage as Message,
    RedisRoute as Route,
    RedisRouter as StreamRouter,
)
from faststream.redis.fastapi import RedisRouter as FastAPIRouter
from faststream.redis.message import RedisMessage as Msg
from faststream.redis.opentelemetry import RedisTelemetryMiddleware
from faststream.redis.prometheus import RedisPrometheusMiddleware


def sync_decoder(msg: Message) -> DecodedMessage:
    return ""


async def async_decoder(msg: Message) -> DecodedMessage:
    return ""


async def custom_decoder(
    msg: Message,
    original: Callable[[Message], Awaitable[DecodedMessage]],
) -> DecodedMessage:
    return await original(msg)


Broker(decoder=sync_decoder)
Broker(decoder=async_decoder)
Broker(decoder=custom_decoder)


def sync_parser(msg: Msg) -> Message:
    return ""  # type: ignore[return-value]


async def async_parser(msg: Msg) -> Message:
    return ""  # type: ignore[return-value]


async def custom_parser(
    msg: Msg,
    original: Callable[[Msg], Awaitable[Message]],
) -> Message:
    return await original(msg)


Broker(parser=sync_parser)
Broker(parser=async_parser)
Broker(parser=custom_parser)


def sync_filter(msg: Message) -> bool:
    return True


async def async_filter(msg: Message) -> bool:
    return True


broker = Broker()


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


StreamRouter(
    parser=sync_parser,
    decoder=sync_decoder,
)
StreamRouter(
    parser=async_parser,
    decoder=async_decoder,
)
StreamRouter(
    parser=custom_parser,
    decoder=custom_decoder,
)


router = StreamRouter()


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


StreamRouter(
    handlers=(
        Route(sync_handler, "test"),
        Route(async_handler, "test"),
        Route(
            sync_handler,
            "test",
            parser=sync_parser,
            decoder=sync_decoder,
        ),
        Route(
            sync_handler,
            "test",
            parser=async_parser,
            decoder=async_decoder,
        ),
        Route(
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


otlp_middleware = RedisTelemetryMiddleware()
Broker().add_middleware(otlp_middleware)
Broker(middlewares=[otlp_middleware])


prometheus_middleware = RedisPrometheusMiddleware(registry=prometheus_client.REGISTRY)
Broker().add_middleware(prometheus_middleware)
Broker(middlewares=[prometheus_middleware])
