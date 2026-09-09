from collections.abc import Mapping
from copy import copy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal
from unittest.mock import MagicMock

from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import ContextError, SetupError

if TYPE_CHECKING:
    from faststream._internal.configs import BrokerConfig
    from faststream.message import StreamMessage


class CallAssertions:
    """The mock and the Call assertions an endpoint answers with under a test broker."""

    __slots__ = ()

    is_test: bool
    _recorder: "CallRecorder"

    @property
    def mock(self) -> MagicMock:
        """The mock recording the endpoint's calls, available under a test broker."""
        return self._recorder_under_test().mock

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
        """Assert the endpoint was called once, with the message described here.

        Args:
            body: The body as a dict, a model or a matcher; it goes through the codec.
            headers: Headers the message must carry; the rest may carry more.
            correlation_id: The exact correlation id.
            reply_to: The exact reply-to destination.
            content_type: The exact content type.
            path: The exact path parameters the subject template matched.
            context: Context paths, as given to `Context()`, mapped to their values.
        """
        await self._assert_call(
            "once",
            body,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type=content_type,
            path=path,
            context=context,
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
        """
        await self._assert_call(
            "last",
            body,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type=content_type,
            path=path,
            context=context,
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
        """
        await self._assert_call(
            "any",
            body,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type=content_type,
            path=path,
            context=context,
        )

    async def _assert_call(
        self,
        which: Literal["once", "last", "any"],
        body: Any,
        /,
        *,
        headers: Any,
        correlation_id: Any,
        reply_to: Any,
        content_type: Any,
        path: Any,
        context: Mapping[str, Any],
    ) -> None:
        recorder = self._recorder_under_test()

        if not recorder.calls:
            msg = f"`{recorder.name}` was not called"
            raise AssertionError(msg)

        if which == "once":
            recorder.mock.assert_called_once()

        candidates = recorder.calls if which == "any" else recorder.calls[-1:]

        reports: list[list[str]] = []
        for call in candidates:
            mismatches = await recorder.mismatches(
                call,
                body,
                headers=headers,
                correlation_id=correlation_id,
                reply_to=reply_to,
                content_type=content_type,
                path=path,
                context=context,
            )
            if not mismatches:
                return
            reports.append(mismatches)

        if which == "any":
            raise AssertionError(_not_called_with(recorder.name, reports))
        raise AssertionError(_called_with_different(recorder.name, reports[0]))

    def _recorder_under_test(self) -> "CallRecorder":
        if not self.is_test:
            msg = (
                f"`{self._recorder.name}` is not under a test broker: "
                "wrap the broker with its `Test*Broker` to access the mock."
            )
            raise SetupError(msg)
        return self._recorder


class CallRecorder:
    """Records the messages an endpoint saw under a test broker and compares them."""

    def __init__(self, name: str, outer_config: "BrokerConfig") -> None:
        self.name = name
        self.mock = MagicMock()
        self.calls: list[RecordedCall] = []

        self._outer_config = outer_config
        # A publisher shares its fake subscriber's recorder, or mirrors a real one
        self._mirrors: list[CallRecorder] = []

    async def record(self, message: "StreamMessage[Any]") -> None:
        # The context is scoped to this call, so it has to be captured now
        # rather than resolved when the assertion runs.
        context = self._outer_config.context.context
        call = RecordedCall(message, context)
        decoded = await message.decode()

        for recorder in (self, *self._mirrors):
            recorder.calls.append(call)
            recorder.mock(decoded)

    def mirror_to(self, other: "CallRecorder") -> None:
        # The test broker may start more than once inside one context
        if other not in self._mirrors:
            self._mirrors.append(other)

    def stop_mirroring(self) -> None:
        self._mirrors.clear()

    def reset(self) -> None:
        self.mock.reset_mock()
        self.calls.clear()

    async def mismatches(
        self,
        call: "RecordedCall",
        body: Any,
        /,
        *,
        headers: Any,
        correlation_id: Any,
        reply_to: Any,
        content_type: Any,
        path: Any,
        context: Mapping[str, Any],
    ) -> list[str]:
        """Compare one Recorded call with the message described, one line per mismatch."""
        checks = _Mismatches()

        if body is not EMPTY:
            checks.compare(
                "body",
                await self._expected_body(body, call.message),
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
                    actual = EMPTY
                checks.compare(f"context[{key!r}]", expected, actual)

        return checks.lines

    async def _expected_body(self, body: Any, message: "StreamMessage[Any]") -> Any:
        """Run the expected body through the path the received one took."""
        codec = self._outer_config.broker_codec or DefaultCodec()
        serializer = self._outer_config.fd_config._serializer

        try:
            if message.batch_headers:
                encoded = [await codec.encode(item, serializer) for item in body]
            else:
                encoded = [await codec.encode(body, serializer)]

        # A matcher (dirty-equals and the like) cannot be encoded: compare it as is
        except (TypeError, ValueError):
            return body

        probes = [
            _with_body(message, data, content_type) for data, content_type in encoded
        ]
        if not message.batch_headers:
            return await probes[0].decode()

        # A batch decoder answers for the whole batch, so items go through the codec
        for probe in probes:
            probe.set_decoder(codec.decode)
        return [await probe.decode() for probe in probes]


@dataclass(slots=True)
class RecordedCall:
    message: "StreamMessage[Any]"
    context: dict[str, Any]


def _with_body(
    message: "StreamMessage[Any]",
    body: bytes,
    content_type: str | None,
) -> "StreamMessage[Any]":
    """A copy of the message carrying another body, decoded the same way."""
    probe = copy(message)
    probe.body = body
    probe.content_type = content_type
    return probe


def _headers_seen_through(expected: Any, actual: dict[str, Any]) -> Any:
    """Project the received headers onto the expected keys."""
    if not isinstance(expected, Mapping):
        return actual
    # Headers match as a subset: the framework and the broker add their own
    return {key: actual.get(key, EMPTY) for key in expected}


def _called_with_different(name: str, mismatches: list[str]) -> str:
    return "\n".join((
        f"`{name}` was called with different arguments:",
        *(f"  {line}" for line in mismatches),
    ))


def _not_called_with(name: str, reports: list[list[str]]) -> str:
    lines = [f"`{name}` was not called with these arguments:"]
    for number, mismatches in enumerate(reports, start=1):
        lines.append(f"  call {number}:")
        lines.extend(f"    {line}" for line in mismatches)
    return "\n".join(lines)


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
        self.lines.append(f"{name}: expected {expected!r}, got {got!r}")
