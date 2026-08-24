import warnings
from typing import TYPE_CHECKING, Any, TypeAlias

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.redis.address import declared_batch
from faststream.redis.schemas import INCORRECT_SETUP_MSG, ListSub, PubSub, StreamSub
from faststream.redis.schemas.proto import validate_options

from .config import RedisSubscriberConfig, RedisSubscriberSpecificationConfig
from .specification import (
    ChannelSubscriberSpecification,
    ListSubscriberSpecification,
    RedisSubscriberSpecification,
    StreamSubscriberSpecification,
)
from .usecases import (
    ChannelConcurrentSubscriber,
    ChannelSubscriber,
    ListBatchSubscriber,
    ListConcurrentSubscriber,
    ListSubscriber,
    LogicSubscriber,
    StreamBatchSubscriber,
    StreamConcurrentSubscriber,
    StreamSubscriber,
)

if TYPE_CHECKING:
    from faststream._internal.config_value import Configurable
    from faststream.redis.configs import RedisBrokerConfig
    from faststream.redis.parser import MessageFormat

SubscriberType: TypeAlias = LogicSubscriber


def create_subscriber(
    *,
    channel: "Configurable[PubSub | str] | None",
    list: "Configurable[ListSub | str] | None",
    stream: "Configurable[StreamSub | str] | None",
    # Subscriber args
    ack_policy: "AckPolicy",
    config: "RedisBrokerConfig",
    no_reply: bool = False,
    message_format: type["MessageFormat"] | None,
    # AsyncAPI args
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
    max_workers: int = 1,
) -> SubscriberType:
    _validate_input_for_misconfigure(
        channel=channel,
        list=list,
        stream=stream,
        ack_policy=ack_policy,
        max_workers=max_workers,
        message_format=message_format,
    )

    subscriber_config = RedisSubscriberConfig(
        channel_sub=channel,
        list_sub=list,
        stream_sub=stream,
        no_reply=no_reply,
        _outer_config=config,
        _ack_policy=ack_policy,
        _message_format=message_format,
    )

    specification_config = RedisSubscriberSpecificationConfig(
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    calls = CallsCollection[Any]()

    specification: RedisSubscriberSpecification
    if subscriber_config.channel_sub:
        specification = ChannelSubscriberSpecification(
            config,
            specification_config,
            calls,
            channel=subscriber_config.channel_sub,
        )

        subscriber_config._ack_policy = AckPolicy.MANUAL

        if max_workers > 1:
            return ChannelConcurrentSubscriber(
                subscriber_config,
                specification,
                calls,
                max_workers=max_workers,
            )

        return ChannelSubscriber(subscriber_config, specification, calls)

    if subscriber_config.stream_sub:
        specification = StreamSubscriberSpecification(
            config,
            specification_config,
            calls,
            stream_sub=subscriber_config.stream_sub,
        )

        if declared_batch(subscriber_config.stream_sub):
            # TODO: raise warning if max_workers in `_validate_input_for_misconfigure`
            return StreamBatchSubscriber(subscriber_config, specification, calls)

        if max_workers > 1:
            return StreamConcurrentSubscriber(
                subscriber_config,
                specification,
                calls,
                max_workers=max_workers,
            )

        return StreamSubscriber(subscriber_config, specification, calls)

    if subscriber_config.list_sub:
        specification = ListSubscriberSpecification(
            config,
            specification_config,
            calls,
            list_sub=subscriber_config.list_sub,
        )

        if declared_batch(subscriber_config.list_sub):
            # TODO: raise warning if max_workers in `_validate_input_for_misconfigure`
            return ListBatchSubscriber(subscriber_config, specification, calls)

        if max_workers > 1:
            return ListConcurrentSubscriber(
                subscriber_config,
                specification,
                calls,
                max_workers=max_workers,
            )

        return ListSubscriber(subscriber_config, specification, calls)

    raise SetupError(INCORRECT_SETUP_MSG)


def _validate_input_for_misconfigure(
    *,
    channel: "Configurable[PubSub | str] | None",
    list: "Configurable[ListSub | str] | None",
    stream: "Configurable[StreamSub | str] | None",
    ack_policy: AckPolicy,
    max_workers: int,
    message_format: type["MessageFormat"] | None,
) -> None:
    validate_options(channel=channel, list=list, stream=stream)

    if stream and ack_policy is AckPolicy.MANUAL and max_workers > 1:
        msg = "Max workers not work with manual no_ack mode."
        raise SetupError(msg)

    if ack_policy is not EMPTY:
        if channel:
            warnings.warn(
                "You can't use acknowledgement policy with PubSub subscriber.",
                RuntimeWarning,
                stacklevel=4,
            )

        if list:
            warnings.warn(
                "You can't use acknowledgement policy with List subscriber.",
                RuntimeWarning,
                stacklevel=4,
            )
