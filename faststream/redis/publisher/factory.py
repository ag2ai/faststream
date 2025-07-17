from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, TypeAlias, Union

from faststream.exceptions import SetupError
from faststream.redis.schemas import INCORRECT_SETUP_MSG, ListSub, PubSub, StreamSub
from faststream.redis.schemas.proto import validate_options

from .config import RedisPublisherConfig, RedisPublisherSpecificationConfig
from .specification import (
    ChannelPublisherSpecification,
    ListPublisherSpecification,
    RedisPublisherSpecification,
    StreamPublisherSpecification,
)
from .usecase import (
    ChannelPublisher,
    ListBatchPublisher,
    ListPublisher,
    StreamPublisher,
)

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import PublisherMiddleware
    from faststream.redis.configs import RedisBrokerConfig


PublisherType: TypeAlias = (
    ChannelPublisher | StreamPublisher | ListPublisher | ListBatchPublisher
)


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
    title_: str | None,
    description_: str | None,
    schema_: Any | None,
    include_in_schema: bool,
) -> PublisherType:
    validate_options(channel=channel, list=list, stream=stream)

    publisher_config = RedisPublisherConfig(
        reply_to=reply_to,
        headers=headers,
        middlewares=middlewares,
        _outer_config=config,
    )

    specification_config = RedisPublisherSpecificationConfig(
        schema_=schema_,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    specification: RedisPublisherSpecification
    if (channel := PubSub.validate(channel)) is not None:
        specification = ChannelPublisherSpecification(
            config,
            specification_config,
            channel,
        )

        return ChannelPublisher(publisher_config, specification, channel=channel)

    if (stream := StreamSub.validate(stream)) is not None:
        specification = StreamPublisherSpecification(
            config,
            specification_config,
            stream,
        )

        return StreamPublisher(publisher_config, specification, stream=stream)

    if (list := ListSub.validate(list)) is not None:
        specification = ListPublisherSpecification(config, specification_config, list)

        if list.batch:
            return ListBatchPublisher(publisher_config, specification, list=list)

        return ListPublisher(publisher_config, specification, list=list)

    raise SetupError(INCORRECT_SETUP_MSG)
