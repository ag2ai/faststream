from collections.abc import Mapping
from typing import Any, ClassVar

import zmqtt

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


class MqttCallAssertions(CallAssertions):
    """The Call assertions of an MQTT endpoint: the message fields, `topic`, `qos`, `retain`."""

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
        topic: Any = EMPTY,
        qos: Any = EMPTY,
        retain: Any = EMPTY,
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
            topic: The exact topic the message arrived on.
            qos: The exact `QoS` the message was delivered with.
            retain: Whether the message was delivered as retained.
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
                topic=topic,
                qos=qos,
                retain=retain,
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
        topic: Any = EMPTY,
        qos: Any = EMPTY,
        retain: Any = EMPTY,
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
            topic: The exact topic the message arrived on.
            qos: The exact `QoS` the message was delivered with.
            retain: Whether the message was delivered as retained.
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
                topic=topic,
                qos=qos,
                retain=retain,
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
        topic: Any = EMPTY,
        qos: Any = EMPTY,
        retain: Any = EMPTY,
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
            topic: The exact topic the message arrived on.
            qos: The exact `QoS` the message was delivered with.
            retain: Whether the message was delivered as retained.
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
                topic=topic,
                qos=qos,
                retain=retain,
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
        topic: Any = EMPTY,
        qos: Any = EMPTY,
        retain: Any = EMPTY,
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
                        ("topic", topic),
                        ("qos", qos),
                        ("retain", retain),
                    )
                    if value is not EMPTY
                },
                type(self)._read_field,
            ),
        )


class MqttHandlerCallWrapper(
    MqttCallAssertions,
    HandlerCallWrapper[P_HandlerParams, T_HandlerReturn],
):
    """The wrapper of an MQTT handler: its Call assertions take `topic`, `qos`, `retain`."""

    __slots__ = ()

    # The message carries the concrete topic it arrived on; `path` holds only the captures
    _read_field = field_reader("MQTT", zmqtt.Message, getattr)
