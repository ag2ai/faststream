from collections.abc import Awaitable, Callable

import prometheus_client
from typing_extensions import assert_type

from faststream._internal.basic_types import DecodedMessage
from faststream.redis import (
    ListSub,
    RedisBroker as Broker,
    RedisChannelMessage,
    RedisListMessage,
    RedisMessage as Message,
    RedisRoute as Route,
    RedisRouter as StreamRouter,
    RedisStreamMessage,
    StreamSub,
)
from faststream.redis.fastapi import RedisRouter as FastAPIRouter
from faststream.redis.message import RedisMessage as Msg
from faststream.redis.opentelemetry import RedisTelemetryMiddleware
from faststream.redis.prometheus import RedisPrometheusMiddleware
from faststream.redis.publisher.usecase import (
    ChannelPublisher,
    ListBatchPublisher,
    ListPublisher,
    StreamPublisher,
)


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


async def async_handler() -> None: ...


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


async def check_publisher_types() -> None:
    broker = Broker()

    p = broker.publisher(channel="test")
    assert_type(p, ChannelPublisher)
    assert_type(await p.publish(None), int)

    p1 = broker.publisher(list="test")
    assert_type(p1, ListPublisher)
    assert_type(await p1.publish(None), int)

    p2 = broker.publisher(list=ListSub("test", batch=True))
    assert_type(p2, ListBatchPublisher | ListPublisher)
    assert_type(await p2.publish(None), int)

    p3 = broker.publisher(stream="stream")
    assert_type(p3, StreamPublisher)
    assert_type(await p3.publish(None), bytes)


async def check_publish_result_type(optional_stream: str | None = "test") -> None:
    broker = Broker()

    publish_with_confirm = await broker.publish(None)
    assert_type(publish_with_confirm, int)

    publish_without_confirm = await broker.publish(None, stream="test")
    assert_type(publish_without_confirm, bytes)

    publish_confirm_bool = await broker.publish(None, stream=optional_stream)
    assert_type(publish_confirm_bool, int | bytes)

    publish_with_confirm = await broker.publish_batch(None, list="test")
    assert_type(publish_with_confirm, int)


async def check_response_type() -> None:
    broker = Broker()

    broker_response = await broker.request(None, "test")
    assert_type(broker_response, RedisChannelMessage)

    p = broker.publisher("test")
    publisher_response = await p.request(None)
    assert_type(publisher_response, RedisChannelMessage)

    p1 = broker.publisher(list="test")
    publisher_response = await p1.request(None)
    assert_type(publisher_response, RedisChannelMessage)

    p2 = broker.publisher(list=ListSub("test", batch=True))
    publisher_response = await p2.request(None)
    assert_type(publisher_response, RedisChannelMessage)

    p3 = broker.publisher(stream="stream")
    publisher_response = await p3.request(None)
    assert_type(publisher_response, RedisChannelMessage)


async def check_channel_subscriber() -> None:
    broker = Broker()

    subscriber = broker.subscriber("test")

    message = await subscriber.get_one()
    assert_type(message, RedisChannelMessage | None)

    async for msg in subscriber:
        assert_type(msg, RedisChannelMessage)


async def check_stream_subscriber() -> None:
    broker = Broker()

    subscriber = broker.subscriber(stream=StreamSub("test"))

    message = await subscriber.get_one()
    assert_type(message, RedisStreamMessage | None)

    async for msg in subscriber:
        assert_type(msg, RedisStreamMessage)


async def check_list_subscriber() -> None:
    broker = Broker()

    subscriber = broker.subscriber(list=ListSub("test"))

    message = await subscriber.get_one()
    assert_type(message, RedisListMessage | None)

    async for msg in subscriber:
        assert_type(msg, RedisListMessage)
