from collections.abc import Awaitable, Callable

import prometheus_client
from redis.asyncio.client import Pipeline
from redis.asyncio.cluster import ClusterPipeline
from typing_extensions import assert_type

from faststream._internal.basic_types import DecodedMessage
from faststream.redis import (
    ListSub,
    PubSub,
    Redis,
    RedisBroker,
    RedisChannelMessage,
    RedisClusterBroker,
    RedisListMessage,
    RedisMessage as Message,
    RedisPublisher,
    RedisRoute as Route,
    RedisRouter,
    RedisSentinelBroker,
    RedisStreamMessage,
    StreamSub,
    TestRedisBroker,
)
from faststream.redis.message import RedisMessage as Msg
from faststream.redis.opentelemetry import RedisTelemetryMiddleware
from faststream.redis.prometheus import RedisPrometheusMiddleware
from faststream.redis.publisher.usecase import (
    ChannelPublisher,
    ListBatchPublisher,
    ListPublisher,
    StreamPublisher,
)
from faststream.redis.subscriber.usecases import (
    ChannelConcurrentSubscriber,
    ChannelSubscriber,
    ListBatchSubscriber,
    ListConcurrentSubscriber,
    ListSubscriber,
    StreamBatchSubscriber,
    StreamConcurrentSubscriber,
    StreamSubscriber,
)


async def check_multiple_test_brokers() -> None:
    async with TestRedisBroker(RedisBroker()) as br1:
        await br1.publish(None, "test")

    async with TestRedisBroker(
        RedisBroker(),
        RedisBroker(),
    ) as (br1, br2):
        await br1.publish(None, "test")
        await br2.publish(None, "test")


def sync_decoder(msg: Message) -> DecodedMessage:
    return ""


async def async_decoder(msg: Message) -> DecodedMessage:
    return ""


async def custom_decoder(
    msg: Message,
    original: Callable[[Message], Awaitable[DecodedMessage]],
) -> DecodedMessage:
    return await original(msg)


RedisBroker(decoder=sync_decoder)
RedisBroker(decoder=async_decoder)
RedisBroker(decoder=custom_decoder)


def sync_parser(msg: Msg) -> Message:
    return ""  # type: ignore[return-value]


async def async_parser(msg: Msg) -> Message:
    return ""  # type: ignore[return-value]


async def custom_parser(
    msg: Msg,
    original: Callable[[Msg], Awaitable[Message]],
) -> Message:
    return await original(msg)


RedisBroker(parser=sync_parser)
RedisBroker(parser=async_parser)
RedisBroker(parser=custom_parser)


def sync_filter(msg: Message) -> bool:
    return True


async def async_filter(msg: Message) -> bool:
    return True


broker = RedisBroker()


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


RedisRouter(
    parser=sync_parser,
    decoder=sync_decoder,
)
RedisRouter(
    parser=async_parser,
    decoder=async_decoder,
)
RedisRouter(
    parser=custom_parser,
    decoder=custom_decoder,
)


router = RedisRouter()

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


RedisRouter(
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


otlp_middleware = RedisTelemetryMiddleware()
RedisBroker().add_middleware(otlp_middleware)
RedisBroker(middlewares=[otlp_middleware])


prometheus_middleware = RedisPrometheusMiddleware(registry=prometheus_client.REGISTRY)
RedisBroker().add_middleware(prometheus_middleware)
RedisBroker(middlewares=[prometheus_middleware])


async def check_broker_publish_result_type(
    broker: RedisBroker,
    pipeline: Pipeline,
    optional_stream: str | None = "test",
) -> None:
    assert_type(await broker.publish(None), int)
    assert_type(await broker.publish(None, pipeline=pipeline), Pipeline)

    assert_type(await broker.publish(None, stream="test"), bytes)
    assert_type(await broker.publish(None, stream="test", pipeline=pipeline), Pipeline)

    assert_type(await broker.publish(None, stream=optional_stream), int | bytes)
    assert_type(
        await broker.publish(None, stream=optional_stream, pipeline=pipeline), Pipeline
    )

    assert_type(await broker.publish_batch(None, list="test"), int)
    assert_type(
        await broker.publish_batch(None, list="test", pipeline=pipeline), Pipeline
    )


async def check_cluster_broker_publish_result_type(
    broker: RedisClusterBroker,
    pipeline: ClusterPipeline,
    optional_stream: str | None = "test",
) -> None:
    assert_type(await broker.publish(None), int)
    assert_type(await broker.publish(None, pipeline=pipeline), ClusterPipeline)

    assert_type(await broker.publish(None, stream="test"), bytes)
    assert_type(
        await broker.publish(None, stream="test", pipeline=pipeline), ClusterPipeline
    )

    assert_type(await broker.publish(None, stream=optional_stream), int | bytes)
    assert_type(
        await broker.publish(None, stream=optional_stream, pipeline=pipeline),
        ClusterPipeline,
    )

    assert_type(await broker.publish_batch(None, list="test"), int)
    assert_type(
        await broker.publish_batch(None, list="test", pipeline=pipeline), ClusterPipeline
    )


async def check_sentinel_broker_publish_result_type(
    broker: RedisSentinelBroker,
    pipeline: Pipeline,
    optional_stream: str | None = "test",
) -> None:
    assert_type(await broker.publish(None), int)
    assert_type(await broker.publish(None, pipeline=pipeline), Pipeline)

    assert_type(await broker.publish(None, stream="test"), bytes)
    assert_type(await broker.publish(None, stream="test", pipeline=pipeline), Pipeline)

    assert_type(await broker.publish(None, stream=optional_stream), int | bytes)
    assert_type(
        await broker.publish(None, stream=optional_stream, pipeline=pipeline), Pipeline
    )

    assert_type(await broker.publish_batch(None, list="test"), int)
    assert_type(
        await broker.publish_batch(None, list="test", pipeline=pipeline), Pipeline
    )


async def check_broker_publisher_publish_result_types(
    broker: RedisBroker,
    pipeline: Pipeline,
) -> None:
    p = broker.publisher(channel="test")
    assert_type(p, ChannelPublisher[Pipeline])
    assert_type(await p.publish(None), int)
    assert_type(await p.publish(None, pipeline=pipeline), Pipeline)

    p1 = broker.publisher(list="test")
    assert_type(p1, ListPublisher[Pipeline])
    assert_type(await p1.publish(None), int)
    assert_type(await p1.publish(None, pipeline=pipeline), Pipeline)

    p2 = broker.publisher(list=ListSub("test", batch=True))
    assert_type(p2, ListBatchPublisher[Pipeline] | ListPublisher[Pipeline])
    assert_type(await p2.publish(None), int)
    assert_type(await p2.publish(None, pipeline=pipeline), Pipeline)

    p3 = broker.publisher(stream="stream")
    assert_type(p3, StreamPublisher[Pipeline])
    assert_type(await p3.publish(None), bytes)
    assert_type(await p3.publish(None, pipeline=pipeline), Pipeline)


async def check_cluster_broker_publisher_publish_result_types(
    broker: RedisClusterBroker,
    pipeline: ClusterPipeline,
) -> None:
    p = broker.publisher(channel="test")
    assert_type(p, ChannelPublisher[ClusterPipeline])
    assert_type(await p.publish(None), int)
    assert_type(await p.publish(None, pipeline=pipeline), ClusterPipeline)

    p1 = broker.publisher(list="test")
    assert_type(p1, ListPublisher[ClusterPipeline])
    assert_type(await p1.publish(None), int)
    assert_type(await p1.publish(None, pipeline=pipeline), ClusterPipeline)

    p2 = broker.publisher(list=ListSub("test", batch=True))
    assert_type(p2, ListBatchPublisher[ClusterPipeline] | ListPublisher[ClusterPipeline])
    assert_type(await p2.publish(None), int)
    assert_type(await p2.publish(None, pipeline=pipeline), ClusterPipeline)

    p3 = broker.publisher(stream="stream")
    assert_type(p3, StreamPublisher[ClusterPipeline])
    assert_type(await p3.publish(None), bytes)
    assert_type(await p3.publish(None, pipeline=pipeline), ClusterPipeline)


async def check_sentinel_broker_publisher_publish_result_types(
    broker: RedisSentinelBroker,
    pipeline: Pipeline,
) -> None:
    p = broker.publisher(channel="test")
    assert_type(p, ChannelPublisher[Pipeline])
    assert_type(await p.publish(None), int)
    assert_type(await p.publish(None, pipeline=pipeline), Pipeline)

    p1 = broker.publisher(list="test")
    assert_type(p1, ListPublisher[Pipeline])
    assert_type(await p1.publish(None), int)
    assert_type(await p1.publish(None, pipeline=pipeline), Pipeline)

    p2 = broker.publisher(list=ListSub("test", batch=True))
    assert_type(p2, ListBatchPublisher[Pipeline] | ListPublisher[Pipeline])
    assert_type(await p2.publish(None), int)
    assert_type(await p2.publish(None, pipeline=pipeline), Pipeline)

    p3 = broker.publisher(stream="stream")
    assert_type(p3, StreamPublisher[Pipeline])
    assert_type(await p3.publish(None), bytes)
    assert_type(await p3.publish(None, pipeline=pipeline), Pipeline)


async def check_router_publisher_publish_result_types(
    router: RedisRouter,
    pipeline: Pipeline,
) -> None:
    p = router.publisher(channel="test")
    assert_type(p, ChannelPublisher[Pipeline | ClusterPipeline])
    assert_type(await p.publish(None), int)
    assert_type(await p.publish(None, pipeline=pipeline), Pipeline | ClusterPipeline)

    p1 = router.publisher(list="test")
    assert_type(p1, ListPublisher[Pipeline | ClusterPipeline])
    assert_type(await p1.publish(None), int)
    assert_type(await p1.publish(None, pipeline=pipeline), Pipeline | ClusterPipeline)

    p2 = router.publisher(list=ListSub("test", batch=True))
    assert_type(
        p2,
        ListBatchPublisher[Pipeline | ClusterPipeline]
        | ListPublisher[Pipeline | ClusterPipeline],
    )
    assert_type(await p2.publish(None), int)
    assert_type(await p2.publish(None, pipeline=pipeline), Pipeline | ClusterPipeline)

    p3 = router.publisher(stream="stream")
    assert_type(p3, StreamPublisher[Pipeline | ClusterPipeline])
    assert_type(await p3.publish(None), bytes)
    assert_type(await p3.publish(None, pipeline=pipeline), Pipeline | ClusterPipeline)


async def check_request_response_type(
    broker: RedisBroker | RedisRouter,
) -> None:
    broker = RedisBroker()

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


async def check_channel_subscriber_message_type(
    broker: RedisBroker | RedisRouter,
) -> None:
    subscriber = broker.subscriber("test")

    message = await subscriber.get_one()
    assert_type(message, RedisChannelMessage | None)

    async for msg in subscriber:
        assert_type(msg, RedisChannelMessage)


async def check_stream_subscriber_message_type(
    broker: RedisBroker | RedisRouter,
    redis: Redis,
) -> None:
    subscriber = broker.subscriber(stream=StreamSub("test"))

    message = await subscriber.get_one()
    assert_type(message, RedisStreamMessage | None)
    if message is not None:
        assert_type(await message.get_delivery_count(redis, "group"), int)

    async for msg in subscriber:
        assert_type(msg, RedisStreamMessage)


async def check_list_subscriber_message_type(
    broker: RedisBroker | RedisRouter,
) -> None:
    subscriber = broker.subscriber(list=ListSub("test"))

    message = await subscriber.get_one()
    assert_type(message, RedisListMessage | None)

    async for msg in subscriber:
        assert_type(msg, RedisListMessage)


def check_channel_subscriber_instance_type(
    broker: RedisBroker | RedisRouter,
) -> None:
    sub1 = broker.subscriber("test")
    assert_type(sub1, ChannelSubscriber)

    sub2 = broker.subscriber(channel="test", max_workers=2)
    assert_type(sub2, ChannelConcurrentSubscriber)


def check_stream_subscriber_instance_type(
    broker: RedisBroker | RedisRouter,
) -> None:
    sub1 = broker.subscriber(stream="test")
    assert_type(sub1, StreamSubscriber)

    sub2 = broker.subscriber(stream=StreamSub("test"))
    assert_type(sub2, StreamSubscriber | StreamBatchSubscriber)

    sub3 = broker.subscriber(stream="test", max_workers=2)
    assert_type(sub3, StreamConcurrentSubscriber)


def check_list_subscriber_instance_type(
    broker: RedisBroker | RedisRouter,
) -> None:
    sub1 = broker.subscriber(list="test")
    assert_type(sub1, ListSubscriber)

    sub2 = broker.subscriber(list=ListSub("test"))
    assert_type(sub2, ListSubscriber | ListBatchSubscriber)

    sub3 = broker.subscriber(list="test", max_workers=2)
    assert_type(sub3, ListConcurrentSubscriber)


RedisBroker(routers=[RedisRouter()])
RedisBroker().include_router(RedisRouter())
RedisBroker().include_routers(RedisRouter())

RedisRouter(routers=[RedisRouter()])
RedisRouter().include_router(RedisRouter())
RedisRouter().include_routers(RedisRouter())


# `RedisPublisher` is documented as a copy of `RedisRegistrator.publisher(...)`
# arguments, so it must accept the same schema objects that method does.
RedisRouter(
    handlers=(
        Route(
            async_handler,
            channel=PubSub("test"),
            publishers=(
                RedisPublisher(channel=PubSub("test")),
                RedisPublisher(list=ListSub("test")),
                RedisPublisher(stream=StreamSub("test")),
            ),
        ),
    ),
)
