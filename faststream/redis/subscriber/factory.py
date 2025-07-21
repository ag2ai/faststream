import warnings
from typing import TYPE_CHECKING, Iterable, Optional, Sequence, Type, Union

from typing_extensions import TypeAlias

from faststream.exceptions import SetupError
from faststream.redis.parser import JSONMessageFormat
from faststream.redis.schemas import INCORRECT_SETUP_MSG, ListSub, PubSub, StreamSub
from faststream.redis.schemas.proto import validate_options
from faststream.redis.subscriber.asyncapi import (
    AsyncAPIChannelSubscriber,
    AsyncAPIListBatchSubscriber,
    AsyncAPIListSubscriber,
    AsyncAPIStreamBatchSubscriber,
    AsyncAPIStreamSubscriber,
)

if TYPE_CHECKING:
    from fast_depends.dependencies import Depends

    from faststream.broker.types import BrokerMiddleware
    from faststream.redis.message import UnifyRedisDict
    from faststream.redis.parser import MessageFormat

SubsciberType: TypeAlias = Union[
    "AsyncAPIChannelSubscriber",
    "AsyncAPIStreamBatchSubscriber",
    "AsyncAPIStreamSubscriber",
    "AsyncAPIListBatchSubscriber",
    "AsyncAPIListSubscriber",
]


def create_subscriber(
    *,
    channel: Union["PubSub", str, None],
    list: Union["ListSub", str, None],
    stream: Union["StreamSub", str, None],
    # Subscriber args
    message_format: Type["MessageFormat"],
    no_ack: bool = False,
    no_reply: bool = False,
    retry: bool = False,
    broker_dependencies: Iterable["Depends"] = (),
    broker_middlewares: Sequence["BrokerMiddleware[UnifyRedisDict]"] = (),
    # AsyncAPI args
    title_: Optional[str] = None,
    description_: Optional[str] = None,
    include_in_schema: bool = True,
) -> SubsciberType:
    validate_options(channel=channel, list=list, stream=stream)

    if message_format == JSONMessageFormat:
        warnings.warn(
            "JSONMessageFormat has been deprecated and will be removed in version 0.7.0 "
            "Instead, use BinaryMessageFormatV1 when creating subscriber",
            category=DeprecationWarning,
            stacklevel=3,
        )

    if (channel_sub := PubSub.validate(channel)) is not None:
        return AsyncAPIChannelSubscriber(
            channel=channel_sub,
            # basic args
            message_format=message_format,
            no_ack=no_ack,
            no_reply=no_reply,
            retry=retry,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    elif (stream_sub := StreamSub.validate(stream)) is not None:
        if stream_sub.batch:
            return AsyncAPIStreamBatchSubscriber(
                stream=stream_sub,
                # basic args
                message_format=message_format,
                no_ack=no_ack,
                no_reply=no_reply,
                retry=retry,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )
        else:
            return AsyncAPIStreamSubscriber(
                stream=stream_sub,
                # basic args
                message_format=message_format,
                no_ack=no_ack,
                no_reply=no_reply,
                retry=retry,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )

    elif (list_sub := ListSub.validate(list)) is not None:
        if list_sub.batch:
            return AsyncAPIListBatchSubscriber(
                list=list_sub,
                # basic args
                message_format=message_format,
                no_ack=no_ack,
                no_reply=no_reply,
                retry=retry,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )
        else:
            return AsyncAPIListSubscriber(
                list=list_sub,
                # basic args
                message_format=message_format,
                no_ack=no_ack,
                no_reply=no_reply,
                retry=retry,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )

    else:
        raise SetupError(INCORRECT_SETUP_MSG)
