from collections.abc import Callable, Mapping
from copy import copy
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Any, TypeAlias
from unittest.mock import MagicMock

from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import ContextError, SetupError

if TYPE_CHECKING:
    from faststream._internal.configs import BrokerConfig
    from faststream.message import StreamMessage

# Takes a Broker field off the raw message, under the name the broker's `publish()` uses
FieldReader: TypeAlias = Callable[[str, "StreamMessage[Any]"], Any]


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
        await self._assert_called_once_with(
            ExpectedCall(
                body=body,
                headers=headers,
                correlation_id=correlation_id,
                reply_to=reply_to,
                content_type=content_type,
                path=path,
                context=context,
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
        await self._assert_called_with(
            ExpectedCall(
                body=body,
                headers=headers,
                correlation_id=correlation_id,
                reply_to=reply_to,
                content_type=content_type,
                path=path,
                context=context,
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
        await self._assert_any_call(
            ExpectedCall(
                body=body,
                headers=headers,
                correlation_id=correlation_id,
                reply_to=reply_to,
                content_type=content_type,
                path=path,
                context=context,
            )
        )

    async def _assert_called_once_with(self, expected: "ExpectedCall") -> None:
        """The check behind `assert_called_once_with`; a broker's mixin enters here."""
        recorder = self._recorder_with_calls()
        recorder.mock.assert_called_once()
        await recorder.assert_last_call(expected)

    async def _assert_called_with(self, expected: "ExpectedCall") -> None:
        await self._recorder_with_calls().assert_last_call(expected)

    async def _assert_any_call(self, expected: "ExpectedCall") -> None:
        await self._recorder_with_calls().assert_any_call(expected)

    def _recorder_with_calls(self) -> "CallRecorder":
        recorder = self._recorder_under_test()
        # Every Call assertion answers alike for an endpoint nobody called
        if not recorder.calls:
            msg = f"`{recorder.name}` was not called"
            raise AssertionError(msg)
        return recorder

    def _recorder_under_test(self) -> "CallRecorder":
        if not self.is_test:
            msg = (
                f"`{self._recorder.name}` is not under a test broker: "
                "wrap the broker with its `Test*Broker` to use the mock "
                "and the Call assertions."
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

    async def assert_last_call(self, expected: "ExpectedCall") -> None:
        """Assert the last Recorded call is the one described."""
        mismatches = await self._mismatches(self.calls[-1], expected)
        if mismatches:
            raise AssertionError(_called_with_different(self.name, mismatches))

    async def assert_any_call(self, expected: "ExpectedCall") -> None:
        """Assert one of the Recorded calls, in order, is the one described."""
        reports: list[list[str]] = []
        for call in self.calls:
            mismatches = await self._mismatches(call, expected)
            if not mismatches:
                return
            reports.append(mismatches)
        raise AssertionError(_not_called_with(self.name, reports))

    async def _mismatches(
        self,
        call: "RecordedCall",
        expected: "ExpectedCall",
    ) -> list[str]:
        """Compare one Recorded call with the message described, one line per mismatch."""
        checks = _Mismatches()

        if expected.body is not EMPTY:
            checks.compare(
                "body",
                await self._expected_body(expected.body, call.message),
                await call.message.decode(),
            )

        asked = expected.message_fields()
        if asked and call.message.batch_headers:
            msg = (
                f"`{self.name}` received a batch: only its body can be asserted, "
                f"not {', '.join(asked)}."
            )
            raise SetupError(msg)

        if expected.headers is not EMPTY:
            checks.compare(
                "headers",
                expected.headers,
                _headers_seen_through(expected.headers, call.message.headers),
                shown=call.message.headers,
            )

        for name in ("correlation_id", "reply_to", "content_type", "path"):
            if (value := getattr(expected, name)) is not EMPTY:
                checks.compare(name, value, getattr(call.message, name))

        if expected.context is not EMPTY:
            repo = ContextRepo(call.context)
            for key, value in expected.context.items():
                try:
                    actual = repo.resolve(key)
                except (ContextError, AttributeError, KeyError):
                    actual = EMPTY
                checks.compare(f"context[{key!r}]", value, actual)

        if (fields := expected.broker_fields) is not None:
            for name, value in fields.values.items():
                checks.compare(name, value, fields.read(name, call.message))

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


@dataclass(slots=True, kw_only=True)
class ExpectedCall:
    """The message a Call assertion describes; a field given as EMPTY is not compared."""

    body: Any
    headers: Any
    correlation_id: Any
    reply_to: Any
    content_type: Any
    path: Any
    context: Mapping[str, Any]
    broker_fields: "BrokerFields | None" = None

    def message_fields(self) -> list[str]:
        """The names asked of the message beside its body."""
        named = (f.name for f in fields(self) if f.name not in {"body", "broker_fields"})
        return [
            *(name for name in named if getattr(self, name) is not EMPTY),
            *(self.broker_fields.values if self.broker_fields else ()),
        ]


@dataclass(slots=True)
class BrokerFields:
    """A broker's own fields a Call assertion asks for, and the reader that answers them."""

    # Under the names the broker's `publish()` takes; a field not asked for is absent
    values: Mapping[str, Any]
    read: FieldReader


def field_reader(
    broker: str,
    record: type,
    read: Callable[[Any, str], Any],
) -> FieldReader:
    """The reader of a broker's raw message: `read(raw, name)` once the message is its.

    Args:
        broker: The broker's name, for the refusal of another broker's message.
        record: The client's message class; a raw message of another class is refused.
        read: Takes the field off the raw message, under the name `publish()` gives it.
    """

    def read_field(name: str, message: "StreamMessage[Any]") -> Any:
        raw = message.raw_message
        if not isinstance(raw, record):
            msg = (
                f"`{name}` is a {broker} field, and this "
                f"`{type(message).__name__}` did not come from {broker}."
            )
            raise SetupError(msg)
        return read(raw, name)

    return read_field


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
