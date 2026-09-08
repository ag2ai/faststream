from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import ContextError, SetupError
from faststream.message import StreamMessage

if TYPE_CHECKING:
    from faststream._internal.configs import BrokerConfig
    from faststream._internal.types import AsyncCallable


class CallRecorder:
    """Records the messages an endpoint saw under a test broker and asserts on them.

    A subscriber handler owns one; a publisher shares the recorder of the fake
    subscriber that receives its messages, or mirrors a real one into its own.
    """

    def __init__(self, name: str, outer_config: "BrokerConfig") -> None:
        self.name = name
        self.mock = MagicMock()
        self.calls: list[RecordedCall] = []

        self._outer_config = outer_config
        self._mirrors: list[CallRecorder] = []

    async def record(
        self,
        message: "StreamMessage[Any]",
        *,
        context: "ContextRepo",
        decoder: "AsyncCallable",
    ) -> None:
        # The context is scoped to this call, so it has to be captured now
        # rather than resolved when the assertion runs.
        call = RecordedCall(message, context.context, decoder)
        self.calls.append(call)
        self.mock(await message.decode())

        for mirror in self._mirrors:
            await mirror.record(message, context=context, decoder=decoder)

    def mirror_to(self, other: "CallRecorder") -> None:
        # The test broker may start more than once inside one context
        if other not in self._mirrors:
            self._mirrors.append(other)

    def stop_mirroring(self) -> None:
        self._mirrors.clear()

    def reset(self) -> None:
        self.mock.reset_mock()
        self.calls.clear()

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
    ) -> None:
        self.mock.assert_called_once()
        call = self.calls[-1]

        checks = _Mismatches()

        if body is not EMPTY:
            checks.compare(
                "body",
                await self._expected_body(body, call),
                await call.message.decode(),
            )

        message_fields = {
            "headers": headers,
            "correlation_id": correlation_id,
            "reply_to": reply_to,
            "content_type": content_type,
            "path": path,
            "context": context,
        }
        asked = [name for name, value in message_fields.items() if value is not EMPTY]
        if asked and call.message.batch_headers:
            msg = (
                f"`{self.name}` received a batch: only its body can be asserted, "
                f"not {', '.join(asked)}."
            )
            raise SetupError(msg)

        if headers is not EMPTY:
            checks.compare(
                "headers",
                headers,
                _headers_seen_through(headers, call.message.headers),
                shown=call.message.headers,
            )

        for name in ("correlation_id", "reply_to", "content_type", "path"):
            if (expected := message_fields[name]) is not EMPTY:
                checks.compare(name, expected, getattr(call.message, name))

        if context is not EMPTY:
            repo = ContextRepo(call.context)
            for key, expected in context.items():
                try:
                    actual = repo.resolve(key)
                except (ContextError, AttributeError, KeyError):
                    actual = _MISSING
                checks.compare(f"context[{key!r}]", expected, actual)

        checks.raise_for(self.name)

    async def _expected_body(self, body: Any, call: "RecordedCall") -> Any:
        """Run the expected body through the path the received one took."""
        codec = self._outer_config.broker_codec or DefaultCodec()
        serializer = self._outer_config.fd_config._serializer

        try:
            if call.message.batch_headers:
                return [
                    await self._decode_as_received(
                        call, codec.decode, *await codec.encode(item, serializer)
                    )
                    for item in body
                ]

            encoded, content_type = await codec.encode(body, serializer)
            return await self._decode_as_received(
                call, call.decoder, encoded, content_type
            )

        # A matcher (dirty-equals and the like) cannot be encoded: compare it as is
        except (TypeError, ValueError):
            return body

    async def _decode_as_received(
        self,
        call: "RecordedCall",
        decoder: "AsyncCallable",
        encoded: bytes,
        content_type: str | None,
    ) -> Any:
        probe: StreamMessage[Any] = StreamMessage(
            raw_message=call.message.raw_message,
            body=encoded,
            headers=call.message.headers,
            content_type=content_type,
            correlation_id=call.message.correlation_id,
            message_id=call.message.message_id,
            reply_to=call.message.reply_to,
            path=call.message.path,
        )
        probe.set_decoder(decoder)
        return await probe.decode()


@dataclass(slots=True)
class RecordedCall:
    message: "StreamMessage[Any]"
    context: dict[str, Any]
    decoder: "AsyncCallable"


class _Missing:
    def __repr__(self) -> str:
        return "<missing>"


_MISSING = _Missing()


def _headers_seen_through(expected: Any, actual: dict[str, Any]) -> Any:
    """Project the received headers onto the expected keys.

    Headers match as a subset because the framework and the broker add their
    own beside the ones a test cares about.
    """
    if not isinstance(expected, Mapping):
        return actual
    return {key: actual.get(key, _MISSING) for key in expected}


@dataclass(slots=True)
class _Mismatches:
    lines: list[str] = field(default_factory=list)

    def compare(
        self,
        name: str,
        expected: Any,
        actual: Any,
        *,
        shown: Any = EMPTY,
    ) -> None:
        if expected == actual:
            return
        got = actual if shown is EMPTY else shown
        self.lines.append(f"  {name}: expected {expected!r}, got {got!r}")

    def raise_for(self, name: str) -> None:
        if self.lines:
            msg = "\n".join(
                (f"`{name}` was called with different arguments:", *self.lines),
            )
            raise AssertionError(msg)
