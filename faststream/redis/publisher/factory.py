from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import TypeAlias

from faststream.exceptions import SetupError
from faststream.redis.schemas import INCORRECT_SETUP_MSG, ListSub, PubSub, StreamSub
from faststream.redis.schemas.proto import validate_options

from .specified import (
    SpecificationChannelPublisher,
    SpecificationListBatchPublisher,
    SpecificationListPublisher,
    SpecificationStreamPublisher,
)

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import BrokerMiddleware, PublisherMiddleware
    from faststream.redis.message import UnifyRedisDict


PublisherType: TypeAlias = Union[
    SpecificationChannelPublisher,
    SpecificationStreamPublisher,
    SpecificationListPublisher,
    SpecificationListBatchPublisher,
]


def create_publisher(
    *,
    channel: Union["PubSub", str, None],
    list: Union["ListSub", str, None],
    stream: Union["StreamSub", str, None],
    headers: Optional["AnyDict"],
    reply_to: str,
    broker_middlewares: Sequence["BrokerMiddleware[UnifyRedisDict]"],
    middlewares: Sequence["PublisherMiddleware"],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    schema_: Optional[Any],
    include_in_schema: bool,
) -> PublisherType:
    validate_options(channel=channel, list=list, stream=stream)

    if (channel := PubSub.validate(channel)) is not None:
        return SpecificationChannelPublisher(
            channel=channel,
            # basic args
            headers=headers,
            reply_to=reply_to,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            schema_=schema_,
            include_in_schema=include_in_schema,
        )

    if (stream := StreamSub.validate(stream)) is not None:
        return SpecificationStreamPublisher(
            stream=stream,
            # basic args
            headers=headers,
            reply_to=reply_to,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            schema_=schema_,
            include_in_schema=include_in_schema,
        )

    if (list := ListSub.validate(list)) is not None:
        if list.batch:
            return SpecificationListBatchPublisher(
                list=list,
                # basic args
                headers=headers,
                reply_to=reply_to,
                broker_middlewares=broker_middlewares,
                middlewares=middlewares,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                schema_=schema_,
                include_in_schema=include_in_schema,
            )
        return SpecificationListPublisher(
            list=list,
            # basic args
            headers=headers,
            reply_to=reply_to,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            schema_=schema_,
            include_in_schema=include_in_schema,
        )

    raise SetupError(INCORRECT_SETUP_MSG)
