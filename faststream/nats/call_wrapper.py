from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from nats.aio.msg import Msg
from nats.js.api import ObjectInfo
from nats.js.kv import KeyValue

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
from faststream._internal.testing.calls import (
    BrokerFields,
    CallAssertions,
    ExpectedCall,
    FieldReader,
    field_reader,
)
from faststream._internal.types import P_HandlerParams, T_HandlerReturn
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from faststream.message import StreamMessage


class NatsCallAssertions(CallAssertions):
    """The Call assertions of a NATS endpoint: the message field `subject`."""

    __slots__ = ()

    # Bound by the wrapper to `_read_subject`, which tells a store entry from a message
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
        subject: Any = EMPTY,
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
            subject: The exact subject the message arrived on.
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
                subject=subject,
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
        subject: Any = EMPTY,
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
            subject: The exact subject the message arrived on.
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
                subject=subject,
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
        subject: Any = EMPTY,
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
            subject: The exact subject the message arrived on.
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
                subject=subject,
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
        subject: Any = EMPTY,
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
                {} if subject is EMPTY else {"subject": subject},
                type(self)._read_field,
            ),
        )


def _read_subject(name: str, message: "StreamMessage[Any]") -> Any:
    """The subject a core or JetStream message arrived on; a store entry has none."""
    raw = message.raw_message
    if isinstance(raw, KeyValue.Entry):
        kind = "a key-value"
    elif isinstance(raw, ObjectInfo):
        kind = "an object-store"
    else:
        return _read_core_field(name, message)
    msg = f"`{name}` is not a field of {kind} message"
    raise SetupError(msg)


_read_core_field = field_reader("NATS", Msg, getattr)


class NatsHandlerCallWrapper(
    NatsCallAssertions,
    HandlerCallWrapper[P_HandlerParams, T_HandlerReturn],
):
    """The wrapper of a NATS handler: its Call assertions take `subject`."""

    __slots__ = ()

    _read_field = _read_subject
