from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from faststream._internal.constants import EMPTY
from faststream._internal.testing.calls import (
    BrokerFields,
    CallAssertions,
    ExpectedCall,
    FieldReader,
)
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from faststream.message import StreamMessage


class KafkaCallAssertions(CallAssertions):
    """The Call assertions of a Kafka endpoint: the message fields, `key` and `partition`."""

    __slots__ = ()

    # Each Kafka package binds the reader of its client's record, see `field_reader`
    _read_field: ClassVar[FieldReader]

    async def assert_called_once_with(
        self,
        body: Any = EMPTY,
        /,
        *,
        headers: Any = EMPTY,
        correlation_id: Any = EMPTY,
        reply_to: Any = EMPTY,
        content_type: Any = EMPTY,
        path: Any = EMPTY,
        context: Mapping[str, Any] = EMPTY,
        key: Any = EMPTY,
        partition: Any = EMPTY,
    ) -> None:
        """Assert the endpoint was called once, with the message described here.

        Args:
            body: The body as a dict, a model or a matcher; it goes through the codec.
            headers: Headers the message must carry; the rest may carry more.
            correlation_id: The exact correlation id.
            reply_to: The exact reply-to destination.
            content_type: The exact content type.
            path: The exact path parameters the subject template matched.
            context: Context paths, as given to `Context()`, mapped to their values.
            key: The exact record key, as the bytes the client delivered.
            partition: The exact partition the record was read from.
        """
        await self._assert_called_once_with(
            self._expected_call(
                body,
                headers=headers,
                correlation_id=correlation_id,
                reply_to=reply_to,
                content_type=content_type,
                path=path,
                context=context,
                key=key,
                partition=partition,
            )
        )

    async def assert_called_with(
        self,
        body: Any = EMPTY,
        /,
        *,
        headers: Any = EMPTY,
        correlation_id: Any = EMPTY,
        reply_to: Any = EMPTY,
        content_type: Any = EMPTY,
        path: Any = EMPTY,
        context: Mapping[str, Any] = EMPTY,
        key: Any = EMPTY,
        partition: Any = EMPTY,
    ) -> None:
        """Assert the last message the endpoint saw is the one described here.

        Args:
            body: The body as a dict, a model or a matcher; it goes through the codec.
            headers: Headers the message must carry; the rest may carry more.
            correlation_id: The exact correlation id.
            reply_to: The exact reply-to destination.
            content_type: The exact content type.
            path: The exact path parameters the subject template matched.
            context: Context paths, as given to `Context()`, mapped to their values.
            key: The exact record key, as the bytes the client delivered.
            partition: The exact partition the record was read from.
        """
        await self._assert_called_with(
            self._expected_call(
                body,
                headers=headers,
                correlation_id=correlation_id,
                reply_to=reply_to,
                content_type=content_type,
                path=path,
                context=context,
                key=key,
                partition=partition,
            )
        )

    async def assert_any_call(
        self,
        body: Any = EMPTY,
        /,
        *,
        headers: Any = EMPTY,
        correlation_id: Any = EMPTY,
        reply_to: Any = EMPTY,
        content_type: Any = EMPTY,
        path: Any = EMPTY,
        context: Mapping[str, Any] = EMPTY,
        key: Any = EMPTY,
        partition: Any = EMPTY,
    ) -> None:
        """Assert one of the messages the endpoint saw is the one described here.

        Args:
            body: The body as a dict, a model or a matcher; it goes through the codec.
            headers: Headers the message must carry; the rest may carry more.
            correlation_id: The exact correlation id.
            reply_to: The exact reply-to destination.
            content_type: The exact content type.
            path: The exact path parameters the subject template matched.
            context: Context paths, as given to `Context()`, mapped to their values.
            key: The exact record key, as the bytes the client delivered.
            partition: The exact partition the record was read from.
        """
        await self._assert_any_call(
            self._expected_call(
                body,
                headers=headers,
                correlation_id=correlation_id,
                reply_to=reply_to,
                content_type=content_type,
                path=path,
                context=context,
                key=key,
                partition=partition,
            )
        )

    def _expected_call(
        self,
        body: Any = EMPTY,
        /,
        *,
        headers: Any = EMPTY,
        correlation_id: Any = EMPTY,
        reply_to: Any = EMPTY,
        content_type: Any = EMPTY,
        path: Any = EMPTY,
        context: Mapping[str, Any] = EMPTY,
        key: Any = EMPTY,
        partition: Any = EMPTY,
    ) -> ExpectedCall:
        return ExpectedCall(
            body=body,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type=content_type,
            path=path,
            context=context,
            broker_fields=BrokerFields(
                {
                    name: value
                    for name, value in (("key", key), ("partition", partition))
                    if value is not EMPTY
                },
                type(self)._read_field,
            ),
        )


def field_reader(
    record: type,
    read: Callable[[Any, str], Any],
) -> FieldReader:
    """The reader of a Kafka client's record: `read(raw, name)` once the record is its.

    Args:
        record: The client's record class; a raw message of another class is refused.
        read: Takes the field off the record, under the name `publish()` gives it.
    """

    def read_field(name: str, message: "StreamMessage[Any]") -> Any:
        raw = message.raw_message
        if not isinstance(raw, record):
            msg = (
                f"`{name}` is a Kafka field, and this "
                f"`{type(message).__name__}` did not come from Kafka."
            )
            raise SetupError(msg)
        return read(raw, name)

    return read_field
