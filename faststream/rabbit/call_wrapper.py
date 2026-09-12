from collections.abc import Mapping
from typing import Any, ClassVar

from aio_pika import IncomingMessage

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


class RabbitCallAssertions(CallAssertions):
    """The Call assertions of a RabbitMQ endpoint: the message's address and identity."""

    __slots__ = ()

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
        exchange: Any = EMPTY,
        routing_key: Any = EMPTY,
        message_id: Any = EMPTY,
        priority: Any = EMPTY,
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
            exchange: The exact exchange name; the default exchange is `""`.
            routing_key: The exact routing key the message was delivered with.
            message_id: The exact message id, as the broker delivered it.
            priority: The exact priority, as the broker delivered it.
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
                exchange=exchange,
                routing_key=routing_key,
                message_id=message_id,
                priority=priority,
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
        exchange: Any = EMPTY,
        routing_key: Any = EMPTY,
        message_id: Any = EMPTY,
        priority: Any = EMPTY,
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
            exchange: The exact exchange name; the default exchange is `""`.
            routing_key: The exact routing key the message was delivered with.
            message_id: The exact message id, as the broker delivered it.
            priority: The exact priority, as the broker delivered it.
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
                exchange=exchange,
                routing_key=routing_key,
                message_id=message_id,
                priority=priority,
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
        exchange: Any = EMPTY,
        routing_key: Any = EMPTY,
        message_id: Any = EMPTY,
        priority: Any = EMPTY,
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
            exchange: The exact exchange name; the default exchange is `""`.
            routing_key: The exact routing key the message was delivered with.
            message_id: The exact message id, as the broker delivered it.
            priority: The exact priority, as the broker delivered it.
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
                exchange=exchange,
                routing_key=routing_key,
                message_id=message_id,
                priority=priority,
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
        exchange: Any = EMPTY,
        routing_key: Any = EMPTY,
        message_id: Any = EMPTY,
        priority: Any = EMPTY,
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
                    for name, value in (
                        ("exchange", exchange),
                        ("routing_key", routing_key),
                        ("message_id", message_id),
                        ("priority", priority),
                    )
                    if value is not EMPTY
                },
                type(self)._read_field,
            ),
        )


class RabbitHandlerCallWrapper(
    RabbitCallAssertions,
    HandlerCallWrapper[P_HandlerParams, T_HandlerReturn],
):
    """The wrapper of a RabbitMQ handler: its Call assertions take the Rabbit fields."""

    __slots__ = ()

    # The test broker delivers a `PatchedMessage`, an `IncomingMessage` subclass,
    # so the guard holds in memory as it does on the wire
    _read_field = field_reader("RabbitMQ", IncomingMessage, getattr)
