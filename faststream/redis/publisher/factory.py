import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, TypeAlias, Union

from faststream.exceptions import SetupError
from faststream.redis.parser import JSONMessageFormat, MessageFormat
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
    LogicPublisher,
    StreamPublisher,
)

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware
    from faststream.redis.configs import RedisBrokerConfig


PublisherType: TypeAlias = LogicPublisher


def create_publisher(
    *,
    channel: Union["PubSub", str, None],
    list: Union["ListSub", str, None],
    stream: Union["StreamSub", str, None],
    headers: dict[str, Any] | None,
    reply_to: str,
    config: "RedisBrokerConfig",
    middlewares: Sequence["PublisherMiddleware"],
    message_format: type["MessageFormat"] | None,
    # AsyncAPI args
    title_: str | None,
    description_: str | None,
    schema_: Any | None,
    include_in_schema: bool,
) -> PublisherType:
    validate_options(channel=channel, list=list, stream=stream)

    if message_format == JSONMessageFormat:
        warnings.warn(
            "JSONMessageFormat has been deprecated and will be removed in version 0.7.0 "
            "Instead, use BinaryMessageFormatV1 when creating publisher",
            category=DeprecationWarning,
            stacklevel=3,
        )

    publisher_config = RedisPublisherConfig(
        reply_to=reply_to,
        headers=headers,
        middlewares=middlewares,
        _message_format=message_format,
        _outer_config=config,
    )

    specification_config = RedisPublisherSpecificationConfig(
        schema_=schema_,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    specification: RedisPublisherSpecification
    if channel_sub := PubSub.validate(channel):
        specification = ChannelPublisherSpecification(
            config,
            specification_config,
            channel_sub,
        )

        return ChannelPublisher(publisher_config, specification, channel=channel_sub)

    if stream_sub := StreamSub.validate(stream):
        specification = StreamPublisherSpecification(
            config,
            specification_config,
            stream_sub,
        )

        return StreamPublisher(publisher_config, specification, stream=stream_sub)

    if list_sub := ListSub.validate(list):
        specification = ListPublisherSpecification(config, specification_config, list_sub)

        if list_sub.batch:
            return ListBatchPublisher(publisher_config, specification, list=list_sub)

        return ListPublisher(publisher_config, specification, list=list_sub)

    raise SetupError(INCORRECT_SETUP_MSG)
