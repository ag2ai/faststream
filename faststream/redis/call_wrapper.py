from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar

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
    from faststream.redis.message import UnifyRedisDict


class RedisCallAssertions(CallAssertions):
    """The Call assertions of a Redis endpoint: its address as `channel`, `list` or `stream`."""

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
        channel: Any = EMPTY,
        list: Any = EMPTY,
        stream: Any = EMPTY,
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
            channel: The exact channel a pub/sub message was delivered on.
            list: The exact list a list message was popped from.
            stream: The exact stream a stream message was read from.
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
                channel=channel,
                list=list,
                stream=stream,
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
        channel: Any = EMPTY,
        list: Any = EMPTY,
        stream: Any = EMPTY,
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
            channel: The exact channel a pub/sub message was delivered on.
            list: The exact list a list message was popped from.
            stream: The exact stream a stream message was read from.
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
                channel=channel,
                list=list,
                stream=stream,
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
        channel: Any = EMPTY,
        list: Any = EMPTY,
        stream: Any = EMPTY,
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
            channel: The exact channel a pub/sub message was delivered on.
            list: The exact list a list message was popped from.
            stream: The exact stream a stream message was read from.
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
                channel=channel,
                list=list,
                stream=stream,
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
        channel: Any = EMPTY,
        list: Any = EMPTY,
        stream: Any = EMPTY,
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
                        ("channel", channel),
                        ("list", list),
                        ("stream", stream),
                    )
                    if value is not EMPTY
                },
                type(self)._read_field,
            ),
        )


# The field each raw `type` answers; every kind stores its address under `channel`
_FIELD_OF_TYPE = {
    "message": "channel",
    "pmessage": "channel",
    "list": "list",
    "blist": "list",
    "stream": "stream",
    "bstream": "stream",
}


def _read_address(raw: "UnifyRedisDict", name: str) -> str:
    field = _FIELD_OF_TYPE[raw["type"]]
    if name != field:
        kind = "pub/sub" if field == "channel" else field
        msg = f"`{name}` was asked of a {kind} message, which answers `{field}`."
        raise SetupError(msg)
    # The subscriber already decoded the address to `str`, whatever the kind
    return raw["channel"]


class RedisHandlerCallWrapper(
    RedisCallAssertions,
    HandlerCallWrapper[P_HandlerParams, T_HandlerReturn],
):
    """The wrapper of a Redis handler: its Call assertions take `channel`, `list`, `stream`."""

    __slots__ = ()

    _read_field = field_reader("Redis", dict, _read_address)
