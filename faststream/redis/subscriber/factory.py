import warnings
from typing import TYPE_CHECKING, Optional, Union

from typing_extensions import TypeAlias

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.redis.configs import RedisSubscriberConfigFacade
from faststream.redis.schemas import INCORRECT_SETUP_MSG, ListSub, PubSub, StreamSub
from faststream.redis.schemas.proto import validate_options
from faststream.redis.subscriber.specified import (
    SpecificationChannelConcurrentSubscriber,
    SpecificationChannelSubscriber,
    SpecificationListBatchSubscriber,
    SpecificationListConcurrentSubscriber,
    SpecificationListSubscriber,
    SpecificationStreamBatchSubscriber,
    SpecificationStreamConcurrentSubscriber,
    SpecificationStreamSubscriber,
)

if TYPE_CHECKING:
    from faststream.redis.configs import RedisBrokerConfig

SubsciberType: TypeAlias = Union[
    SpecificationChannelSubscriber,
    SpecificationStreamBatchSubscriber,
    SpecificationStreamSubscriber,
    SpecificationListBatchSubscriber,
    SpecificationListSubscriber,
    SpecificationChannelConcurrentSubscriber,
    SpecificationListConcurrentSubscriber,
    SpecificationStreamConcurrentSubscriber,
]


def create_subscriber(
    *,
    channel: Union["PubSub", str, None],
    list: Union["ListSub", str, None],
    stream: Union["StreamSub", str, None],
    # Subscriber args
    ack_policy: "AckPolicy",
    no_ack: bool,
    config: "RedisBrokerConfig",
    no_reply: bool = False,
    # AsyncAPI args
    title_: Optional[str] = None,
    description_: Optional[str] = None,
    include_in_schema: bool = True,
    max_workers: int = 1,
) -> SubsciberType:
    _validate_input_for_misconfigure(
        channel=channel,
        list=list,
        stream=stream,
        ack_policy=ack_policy,
        no_ack=no_ack,
        max_workers=max_workers,
    )

    config = RedisSubscriberConfigFacade(
        channel_sub=PubSub.validate(channel),
        list_sub=ListSub.validate(list),
        stream_sub=StreamSub.validate(stream),
        no_reply=no_reply,
        config=config,
        _ack_policy=ack_policy,
        _no_ack=no_ack,
        # specification
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    if config.channel_sub:
        config._ack_policy = AckPolicy.DO_NOTHING

        if max_workers > 1:
            return SpecificationChannelConcurrentSubscriber(
                config,
                max_workers=max_workers,
            )

        return SpecificationChannelSubscriber(config)

    if config.stream_sub:
        if config.stream_sub.batch:
            return SpecificationStreamBatchSubscriber(config)

        if max_workers > 1:
            return SpecificationStreamConcurrentSubscriber(
                config,
                max_workers=max_workers,
            )

        return SpecificationStreamSubscriber(config)

    if config.list_sub:
        if config.list_sub.batch:
            return SpecificationListBatchSubscriber(config)

        if max_workers > 1:
            return SpecificationListConcurrentSubscriber(
                config,
                max_workers=max_workers,
            )

        return SpecificationListSubscriber(config)

    raise SetupError(INCORRECT_SETUP_MSG)


def _validate_input_for_misconfigure(
    *,
    channel: Union["PubSub", str, None],
    list: Union["ListSub", str, None],
    stream: Union["StreamSub", str, None],
    ack_policy: AckPolicy,
    no_ack: bool,
    max_workers: int,
) -> None:
    validate_options(channel=channel, list=list, stream=stream)

    if no_ack is not EMPTY:
        warnings.warn(
            "`no_ack` option was deprecated in prior to `ack_policy=AckPolicy.DO_NOTHING`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `no_ack` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

    if stream and no_ack and max_workers > 1:
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
