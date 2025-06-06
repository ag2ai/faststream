from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import TypeAlias

from faststream.exceptions import SetupError
from faststream.redis.configs import RedisPublisherConfigFacade
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
    from faststream._internal.types import PublisherMiddleware
    from faststream.redis.configs import RedisBrokerConfig


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
    config: "RedisBrokerConfig",
    middlewares: Sequence["PublisherMiddleware"],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    schema_: Optional[Any],
    include_in_schema: bool,
) -> PublisherType:
    validate_options(channel=channel, list=list, stream=stream)

    config = RedisPublisherConfigFacade(
        reply_to=reply_to,
        headers=headers,
        config=config,
        middlewares=middlewares,
        # specification
        schema_=schema_,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    if (channel := PubSub.validate(channel)) is not None:
        return SpecificationChannelPublisher(config, channel=channel)

    if (stream := StreamSub.validate(stream)) is not None:
        return SpecificationStreamPublisher(config, stream=stream)

    if (list := ListSub.validate(list)) is not None:
        if list.batch:
            return SpecificationListBatchPublisher(config, list=list)

        return SpecificationListPublisher(config, list=list)

    raise SetupError(INCORRECT_SETUP_MSG)
