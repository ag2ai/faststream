import warnings
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Type, Union

from typing_extensions import TypeAlias, override

from faststream.asyncapi.schema import (
    Channel,
    ChannelBinding,
    CorrelationId,
    Message,
    Operation,
)
from faststream.asyncapi.schema.bindings import redis
from faststream.asyncapi.utils import resolve_payloads
from faststream.exceptions import SetupError
from faststream.redis.parser import JSONMessageFormat, MessageFormat
from faststream.redis.publisher.usecase import (
    ChannelPublisher,
    ListBatchPublisher,
    ListPublisher,
    LogicPublisher,
    StreamPublisher,
)
from faststream.redis.schemas import INCORRECT_SETUP_MSG, ListSub, PubSub, StreamSub
from faststream.redis.schemas.proto import RedisAsyncAPIProtocol, validate_options

if TYPE_CHECKING:
    from faststream.broker.types import BrokerMiddleware, PublisherMiddleware
    from faststream.redis.message import UnifyRedisDict
    from faststream.types import AnyDict

PublisherType: TypeAlias = Union[
    "AsyncAPIChannelPublisher",
    "AsyncAPIStreamPublisher",
    "AsyncAPIListPublisher",
    "AsyncAPIListBatchPublisher",
]


class AsyncAPIPublisher(LogicPublisher, RedisAsyncAPIProtocol):
    """A class to represent a Redis publisher."""

    def get_schema(self) -> Dict[str, Channel]:
        payloads = self.get_payloads()

        return {
            self.name: Channel(
                description=self.description,
                publish=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                        correlationId=CorrelationId(
                            location="$message.header#/correlation_id"
                        ),
                    ),
                ),
                bindings=ChannelBinding(
                    redis=self.channel_binding,
                ),
            )
        }

    @override
    @staticmethod
    def create(  # type: ignore[override]
        *,
        channel: Union["PubSub", str, None],
        list: Union["ListSub", str, None],
        stream: Union["StreamSub", str, None],
        headers: Optional["AnyDict"],
        reply_to: str,
        broker_middlewares: Sequence["BrokerMiddleware[UnifyRedisDict]"],
        middlewares: Sequence["PublisherMiddleware"],
        message_format: Type["MessageFormat"],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        schema_: Optional[Any],
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

        if (channel := PubSub.validate(channel)) is not None:
            return AsyncAPIChannelPublisher(
                channel=channel,
                # basic args
                headers=headers,
                reply_to=reply_to,
                broker_middlewares=broker_middlewares,
                middlewares=middlewares,
                message_format=message_format,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                schema_=schema_,
                include_in_schema=include_in_schema,
            )

        if (stream := StreamSub.validate(stream)) is not None:
            return AsyncAPIStreamPublisher(
                stream=stream,
                # basic args
                headers=headers,
                reply_to=reply_to,
                broker_middlewares=broker_middlewares,
                middlewares=middlewares,
                message_format=message_format,
                # AsyncAPI args
                title_=title_,
                description_=description_,
                schema_=schema_,
                include_in_schema=include_in_schema,
            )

        if (list := ListSub.validate(list)) is not None:
            if list.batch:
                return AsyncAPIListBatchPublisher(
                    list=list,
                    # basic args
                    headers=headers,
                    reply_to=reply_to,
                    broker_middlewares=broker_middlewares,
                    middlewares=middlewares,
                    message_format=message_format,
                    # AsyncAPI args
                    title_=title_,
                    description_=description_,
                    schema_=schema_,
                    include_in_schema=include_in_schema,
                )
            else:
                return AsyncAPIListPublisher(
                    list=list,
                    # basic args
                    headers=headers,
                    reply_to=reply_to,
                    broker_middlewares=broker_middlewares,
                    middlewares=middlewares,
                    message_format=message_format,
                    # AsyncAPI args
                    title_=title_,
                    description_=description_,
                    schema_=schema_,
                    include_in_schema=include_in_schema,
                )

        raise SetupError(INCORRECT_SETUP_MSG)


class AsyncAPIChannelPublisher(ChannelPublisher, AsyncAPIPublisher):
    def get_name(self) -> str:
        return f"{self.channel.name}:Publisher"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.channel.name,
            method="publish",
        )


class _ListPublisherMixin(AsyncAPIPublisher):
    list: "ListSub"

    def get_name(self) -> str:
        return f"{self.list.name}:Publisher"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.list.name,
            method="rpush",
        )


class AsyncAPIListPublisher(ListPublisher, _ListPublisherMixin):
    pass


class AsyncAPIListBatchPublisher(ListBatchPublisher, _ListPublisherMixin):
    pass


class AsyncAPIStreamPublisher(StreamPublisher, AsyncAPIPublisher):
    def get_name(self) -> str:
        return f"{self.stream.name}:Publisher"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.stream.name,
            method="xadd",
        )
