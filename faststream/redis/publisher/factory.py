from typing import TYPE_CHECKING, Any, TypeAlias

from faststream._internal.config_value import Configurable
from faststream.exceptions import SetupError
from faststream.redis.address import declared_batch
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
    from faststream.redis.configs import RedisBrokerConfig
    from faststream.redis.parser import MessageFormat


PublisherType: TypeAlias = LogicPublisher


def create_publisher(
    *,
    channel: "Configurable[PubSub | str] | None",
    list: "Configurable[ListSub | str] | None",
    stream: "Configurable[StreamSub | str] | None",
    headers: dict[str, Any] | None,
    reply_to: Configurable[str],
    config: "RedisBrokerConfig",
    message_format: type["MessageFormat"] | None,
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
    if channel:
        specification = ChannelPublisherSpecification(
            config,
            specification_config,
            channel,
        )

        return ChannelPublisher(publisher_config, specification, channel=channel)

    if stream:
        specification = StreamPublisherSpecification(
            config,
            specification_config,
            stream,
        )

        return StreamPublisher(publisher_config, specification, stream=stream)

    if list:
        specification = ListPublisherSpecification(config, specification_config, list)

        if declared_batch(list):
            return ListBatchPublisher(publisher_config, specification, list=list)

        return ListPublisher(publisher_config, specification, list=list)

    raise SetupError(INCORRECT_SETUP_MSG)
