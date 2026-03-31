from collections.abc import Awaitable, Callable

import prometheus_client
from typing_extensions import assert_type

from faststream._internal.basic_types import DecodedMessage
from faststream.mq import MQBroker, MQMessage, MQRoute, MQRouter
from faststream.mq.fastapi import MQRouter as FastAPIRouter
from faststream.mq.opentelemetry import MQTelemetryMiddleware
from faststream.mq.prometheus import MQPrometheusMiddleware
from faststream.mq.prometheus.provider import MQMetricsSettingsProvider
from faststream.mq.opentelemetry.provider import MQTelemetrySettingsProvider
from faststream.mq.publisher.usecase import MQPublisher
from faststream.mq.subscriber.usecase import MQSubscriber


def sync_decoder(msg: MQMessage) -> DecodedMessage:
    return ""


async def async_decoder(msg: MQMessage) -> DecodedMessage:
    return ""


async def custom_decoder(
    msg: MQMessage,
    original: Callable[[MQMessage], Awaitable[DecodedMessage]],
) -> DecodedMessage:
    return await original(msg)


MQBroker(queue_manager="QM1", decoder=sync_decoder)
MQBroker(queue_manager="QM1", decoder=async_decoder)
MQBroker(queue_manager="QM1", decoder=custom_decoder)


def sync_parser(msg: MQMessage) -> MQMessage:
    return msg


async def async_parser(msg: MQMessage) -> MQMessage:
    return msg


async def custom_parser(
    msg: MQMessage,
    original: Callable[[MQMessage], Awaitable[MQMessage]],
) -> MQMessage:
    return await original(msg)


MQBroker(queue_manager="QM1", parser=sync_parser)
MQBroker(queue_manager="QM1", parser=async_parser)
MQBroker(queue_manager="QM1", parser=custom_parser)


def sync_filter(msg: MQMessage) -> bool:
    return True


async def async_filter(msg: MQMessage) -> bool:
    return True


broker = MQBroker(queue_manager="QM1")
sub = broker.subscriber("test")


@sub(filter=sync_filter)
async def handle() -> None: ...


@sub(filter=async_filter)
async def handle2() -> None: ...


@broker.subscriber("test", parser=sync_parser, decoder=sync_decoder)
async def handle3() -> None: ...


@broker.subscriber("test")
@broker.publisher("test2")
async def handle4() -> None: ...


router = MQRouter()
router_sub = router.subscriber("test")


@router_sub(filter=sync_filter)
async def handle5() -> None: ...


MQRouter(
    handlers=(
        MQRoute(handle5, "test"),
        MQRoute(handle5, "test", parser=sync_parser, decoder=sync_decoder),
    ),
)

fastapi_router = FastAPIRouter(queue_manager="QM1")


@fastapi_router.subscriber("test")
async def handle6() -> None: ...


publisher = broker.publisher("test")
assert_type(publisher, MQPublisher)
assert_type(sub, MQSubscriber)
assert_type(prometheus_client.Counter, object)
assert_type(MQMetricsSettingsProvider, type[MQMetricsSettingsProvider])
assert_type(MQTelemetrySettingsProvider, type[MQTelemetrySettingsProvider])
assert_type(MQPrometheusMiddleware, type[MQPrometheusMiddleware])
assert_type(MQTelemetryMiddleware, type[MQTelemetryMiddleware])
