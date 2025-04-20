import warnings
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Optional, Union

from typing_extensions import TypeAlias

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
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
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import BrokerMiddleware
    from faststream.redis.message import UnifyRedisDict

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
    no_reply: bool = False,
    broker_dependencies: Iterable["Dependant"] = (),
    broker_middlewares: Sequence["BrokerMiddleware[UnifyRedisDict]"] = (),
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

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.DO_NOTHING if no_ack else AckPolicy.REJECT_ON_ERROR

    if (channel_sub := PubSub.validate(channel)) is not None:
        if max_workers > 1:
            return SpecificationChannelConcurrentSubscriber(
                channel=channel_sub,
                # basic args
                no_reply=no_reply,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                max_workers=max_workers,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )
        return SpecificationChannelSubscriber(
            channel=channel_sub,
            # basic args
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    if (stream_sub := StreamSub.validate(stream)) is not None:
        if stream_sub.batch:
            return SpecificationStreamBatchSubscriber(
                stream=stream_sub,
                # basic args
                ack_policy=ack_policy,
                no_reply=no_reply,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )
        if max_workers > 1:
            return SpecificationStreamConcurrentSubscriber(
                stream=stream_sub,
                # basic args
                ack_policy=ack_policy,
                no_reply=no_reply,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                max_workers=max_workers,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )
        return SpecificationStreamSubscriber(
            stream=stream_sub,
            # basic args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    if (list_sub := ListSub.validate(list)) is not None:
        if list_sub.batch:
            return SpecificationListBatchSubscriber(
                list=list_sub,
                # basic args
                no_reply=no_reply,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )
        if max_workers > 1:
            return SpecificationListConcurrentSubscriber(
                list=list_sub,
                # basic args
                no_reply=no_reply,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                max_workers=max_workers,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )
        return SpecificationListSubscriber(
            list=list_sub,
            # basic args
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

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
