import warnings
from typing import TYPE_CHECKING, Any, TypeAlias, Union

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
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
    ChannelSubscriber,
    ListBatchSubscriber,
    ListSubscriber,
    LogicSubscriber,
    StreamBatchSubscriber,
    StreamSubscriber,
)

if TYPE_CHECKING:
    from faststream.redis.configs import RedisBrokerConfig
    from faststream.redis.parser import MessageFormat

SubscriberType: TypeAlias = LogicSubscriber


def create_subscriber(
    *,
    channel: Union["PubSub", str, None],
    list: Union["ListSub", str, None],
    stream: Union["StreamSub", str, None],
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
    )

    subscriber_config = RedisSubscriberConfig(
        channel_sub=PubSub.validate(channel),
        list_sub=ListSub.validate(list),
        stream_sub=StreamSub.validate(stream),
        no_reply=no_reply,
        max_workers=max_workers,
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
        return ChannelSubscriber(subscriber_config, specification, calls)

    if subscriber_config.stream_sub:
        specification = StreamSubscriberSpecification(
            config,
            specification_config,
            calls,
            stream_sub=subscriber_config.stream_sub,
        )

        if subscriber_config.stream_sub.batch:
            subscriber_config.max_workers = 1
            return StreamBatchSubscriber(subscriber_config, specification, calls)

        return StreamSubscriber(subscriber_config, specification, calls)

    if subscriber_config.list_sub:
        specification = ListSubscriberSpecification(
            config,
            specification_config,
            calls,
            list_sub=subscriber_config.list_sub,
        )

        if subscriber_config.list_sub.batch:
            subscriber_config.max_workers = 1
            return ListBatchSubscriber(subscriber_config, specification, calls)

        return ListSubscriber(subscriber_config, specification, calls)

    raise SetupError(INCORRECT_SETUP_MSG)


def _validate_input_for_misconfigure(
    *,
    channel: Union["PubSub", str, None],
    list: Union["ListSub", str, None],
    stream: Union["StreamSub", str, None],
    ack_policy: AckPolicy,
    max_workers: int,
) -> None:
    validate_options(channel=channel, list=list, stream=stream)

    if max_workers > 1 and (
        (isinstance(list, ListSub) and list.batch)
        or (isinstance(stream, StreamSub) and stream.batch)
    ):
        warnings.warn(
            "The `max_workers` option is ignored by a batch subscriber.",
            RuntimeWarning,
            stacklevel=4,
        )

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
