from collections.abc import Awaitable, Callable

import prometheus_client
from nats.aio.msg import Msg
from typing_extensions import assert_type

from faststream._internal.basic_types import DecodedMessage
from faststream.nats import NatsBroker, NatsMessage, NatsRoute, NatsRouter, PubAck
from faststream.nats.fastapi import NatsRouter as FastAPIRouter
from faststream.nats.message import NatsKvMessage, NatsObjMessage
from faststream.nats.opentelemetry import NatsTelemetryMiddleware
from faststream.nats.prometheus import NatsPrometheusMiddleware
from faststream.nats.publisher.usecase import LogicPublisher
from faststream.nats.schemas import ObjWatch, PullSub
from faststream.nats.subscriber.usecases import (
    BatchPullStreamSubscriber,
    ConcurrentCoreSubscriber,
    ConcurrentPullStreamSubscriber,
    ConcurrentPushStreamSubscriber,
    CoreSubscriber,
    KeyValueWatchSubscriber,
    ObjStoreWatchSubscriber,
    PullStreamSubscriber,
    PushStreamSubscriber,
)


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


async def async_handler() -> None: ...


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


async def check_broker_publish_result_type() -> None:
    broker = NatsBroker()

    assert_type(await broker.publish(None, "test"), None)
    assert_type(await broker.publish(None, "test", stream="stream"), PubAck)


async def check_publisher_publish_result_type() -> None:
    broker = NatsBroker()

    publisher = broker.publisher("test")

    assert_type(await publisher.publish(None, "test"), None)
    assert_type(await publisher.publish(None, "test", stream="stream"), PubAck)


async def check_request_response_type() -> None:
    broker = NatsBroker()

    broker_response = await broker.request(None, "test")
    assert_type(broker_response, NatsMessage)

    publisher = broker.publisher("test")
    assert_type(await publisher.request(None, "test"), NatsMessage)


async def check_core_subscriber_message_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    subscriber = broker.subscriber("test")

    message = await subscriber.get_one()
    assert_type(message, NatsMessage | None)

    async for msg in subscriber:
        assert_type(msg, NatsMessage)


async def check_concurrent_core_subscriber_message_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    subscriber = broker.subscriber("test", max_workers=2)

    message = await subscriber.get_one()
    assert_type(message, NatsMessage | None)

    async for msg in subscriber:
        assert_type(msg, NatsMessage)


async def check_push_stream_subscriber_message_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    subscriber = broker.subscriber("test", stream="stream")

    message = await subscriber.get_one()
    assert_type(message, NatsMessage | None)

    async for msg in subscriber:
        assert_type(msg, NatsMessage)


async def check_concurrent_push_stream_subscriber_message_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    subscriber = broker.subscriber("test", stream="stream", max_workers=2)

    message = await subscriber.get_one()
    assert_type(message, NatsMessage | None)

    async for msg in subscriber:
        assert_type(msg, NatsMessage)


async def check_pull_stream_subscriber_message_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    subscriber = broker.subscriber("test", stream="stream", pull_sub=True)

    message = await subscriber.get_one()
    assert_type(message, NatsMessage | None)

    async for msg in subscriber:
        assert_type(msg, NatsMessage)


async def check_concurrent_pull_stream_subscriber_message_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    subscriber = broker.subscriber("test", stream="stream", pull_sub=True, max_workers=2)

    message = await subscriber.get_one()
    assert_type(message, NatsMessage | None)

    async for msg in subscriber:
        assert_type(msg, NatsMessage)


async def check_batch_pull_stream_subscriber_message_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    subscriber = broker.subscriber(
        "test",
        stream="stream",
        pull_sub=PullSub(batch=True),
    )

    message = await subscriber.get_one()
    assert_type(message, NatsMessage | None)

    async for msg in subscriber:
        assert_type(msg, NatsMessage)


async def check_key_value_watch_subscriber_message_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    subscriber = broker.subscriber("key", kv_watch="bucket")

    message = await subscriber.get_one()
    assert_type(message, NatsKvMessage | None)

    async for msg in subscriber:
        assert_type(msg, NatsKvMessage)


async def check_object_store_watch_subscriber_message_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    subscriber = broker.subscriber("key", obj_watch=ObjWatch())

    message = await subscriber.get_one()
    assert_type(message, NatsObjMessage | None)

    async for msg in subscriber:
        assert_type(msg, NatsObjMessage)


def check_subscriber_instance_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    sub1 = broker.subscriber("key", kv_watch="bucket")
    assert_type(sub1, KeyValueWatchSubscriber)

    sub2 = broker.subscriber("key", obj_watch=ObjWatch())
    assert_type(sub2, ObjStoreWatchSubscriber)

    sub3 = broker.subscriber(
        "test",
        stream="stream",
        pull_sub=PullSub(batch=True),
    )
    assert_type(sub3, BatchPullStreamSubscriber | PullStreamSubscriber)

    sub4 = broker.subscriber("test", stream="stream", pull_sub=True, max_workers=2)
    assert_type(sub4, ConcurrentPullStreamSubscriber)

    sub5 = broker.subscriber("test", stream="stream", pull_sub=True)
    assert_type(sub5, PullStreamSubscriber)

    sub6 = broker.subscriber("test", stream="stream", max_workers=2)
    assert_type(sub6, ConcurrentPushStreamSubscriber)

    sub7 = broker.subscriber("test", stream="stream")
    assert_type(sub7, PushStreamSubscriber)

    sub8 = broker.subscriber("test", max_workers=2)
    assert_type(sub8, ConcurrentCoreSubscriber)

    sub9 = broker.subscriber("test")
    assert_type(sub9, CoreSubscriber)


def check_publisher_instance_type(
    broker: NatsBroker | FastAPIRouter | NatsRouter,
) -> None:
    publisher = broker.publisher("test")
    assert_type(publisher, LogicPublisher)


NatsBroker(routers=[NatsRouter()])
NatsBroker().include_router(NatsRouter())
NatsBroker().include_routers(NatsRouter())

NatsRouter(routers=[NatsRouter()])
NatsRouter().include_router(NatsRouter())
NatsRouter().include_routers(NatsRouter())
