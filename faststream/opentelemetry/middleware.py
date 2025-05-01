import time
from collections import defaultdict
from copy import copy
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Type, cast

from opentelemetry import baggage, context, metrics, trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Link, Span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from faststream import BaseMiddleware
from faststream import context as fs_context
from faststream.opentelemetry.baggage import Baggage
from faststream.opentelemetry.consts import (
    ERROR_TYPE,
    INSTRUMENTING_LIBRARY_VERSION,
    INSTRUMENTING_MODULE_NAME,
    MESSAGING_DESTINATION_PUBLISH_NAME,
    OTEL_SCHEMA,
    WITH_BATCH,
    MessageAction,
)
from faststream.opentelemetry.provider import TelemetrySettingsProvider

if TYPE_CHECKING:
    from contextvars import Token
    from types import TracebackType

    from opentelemetry.metrics import Meter, MeterProvider
    from opentelemetry.trace import Tracer, TracerProvider
    from opentelemetry.util.types import Attributes

    from faststream.broker.message import StreamMessage
    from faststream.types import AnyDict, AsyncFunc, AsyncFuncAny


_BAGGAGE_PROPAGATOR = W3CBaggagePropagator()
_TRACE_PROPAGATOR = TraceContextTextMapPropagator()


class _MetricsContainer:
    __slots__ = (
        "include_messages_counters",
        "process_counter",
        "process_duration",
        "publish_counter",
        "publish_duration",
    )

    def __init__(self, meter: "Meter", include_messages_counters: bool) -> None:
        self.include_messages_counters = include_messages_counters

        self.publish_duration = meter.create_histogram(
            name="messaging.publish.duration",
            unit="s",
            description="Measures the duration of publish operation.",
        )
        self.process_duration = meter.create_histogram(
            name="messaging.process.duration",
            unit="s",
            description="Measures the duration of process operation.",
        )

        if include_messages_counters:
            self.process_counter = meter.create_counter(
                name="messaging.process.messages",
                unit="message",
                description="Measures the number of processed messages.",
            )
            self.publish_counter = meter.create_counter(
                name="messaging.publish.messages",
                unit="message",
                description="Measures the number of published messages.",
            )

    def observe_publish(
        self, attrs: "AnyDict", duration: float, msg_count: int
    ) -> None:
        self.publish_duration.record(
            amount=duration,
            attributes=attrs,
        )
        if self.include_messages_counters:
            counter_attrs = copy(attrs)
            counter_attrs.pop(ERROR_TYPE, None)
            self.publish_counter.add(
                amount=msg_count,
                attributes=counter_attrs,
            )

    def observe_consume(
        self, attrs: "AnyDict", duration: float, msg_count: int
    ) -> None:
        self.process_duration.record(
            amount=duration,
            attributes=attrs,
        )
        if self.include_messages_counters:
            counter_attrs = copy(attrs)
            counter_attrs.pop(ERROR_TYPE, None)
            self.process_counter.add(
                amount=msg_count,
                attributes=counter_attrs,
            )


class BaseTelemetryMiddleware(BaseMiddleware):
    def __init__(
        self,
        *,
        tracer: "Tracer",
        settings_provider_factory: Callable[
            [Any], Optional[TelemetrySettingsProvider[Any]]
        ],
        metrics_container: _MetricsContainer,
        msg: Optional[Any] = None,
    ) -> None:
        self.msg = msg

        self._tracer = tracer
        self._metrics = metrics_container
        self._current_span: Optional[Span] = None
        self._origin_context: Optional[Context] = None
        self._scope_tokens: List[Tuple[str, Token[Any]]] = []
        self.__settings_provider = settings_provider_factory(msg)

    async def publish_scope(
        self,
        call_next: "AsyncFunc",
        msg: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if (provider := self.__settings_provider) is None:
            return await call_next(msg, *args, **kwargs)

        headers = kwargs.pop("headers", {}) or {}
        current_context = context.get_current()
        destination_name = provider.get_publish_destination_name(kwargs)

        current_baggage: Optional[Baggage] = fs_context.get_local("baggage")
        if current_baggage:
            headers.update(current_baggage.to_headers())

        trace_attributes = provider.get_publish_attrs_from_kwargs(kwargs)
        metrics_attributes = {
            SpanAttributes.MESSAGING_SYSTEM: provider.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: destination_name,
        }

        # NOTE: if batch with single message?
        if (msg_count := len((msg, *args))) > 1:
            trace_attributes[SpanAttributes.MESSAGING_BATCH_MESSAGE_COUNT] = msg_count
            current_context = _BAGGAGE_PROPAGATOR.extract(headers, current_context)
            _BAGGAGE_PROPAGATOR.inject(
                headers, baggage.set_baggage(WITH_BATCH, True, context=current_context)
            )

        if self._current_span and self._current_span.is_recording():
            current_context = trace.set_span_in_context(
                self._current_span, current_context
            )
            _TRACE_PROPAGATOR.inject(headers, context=self._origin_context)

        else:
            create_span = self._tracer.start_span(
                name=_create_span_name(destination_name, MessageAction.CREATE),
                kind=trace.SpanKind.PRODUCER,
                attributes=trace_attributes,
            )
            current_context = trace.set_span_in_context(create_span)
            _TRACE_PROPAGATOR.inject(headers, context=current_context)
            create_span.end()

        start_time = time.perf_counter()

        try:
            with self._tracer.start_as_current_span(
                name=_create_span_name(destination_name, MessageAction.PUBLISH),
                kind=trace.SpanKind.PRODUCER,
                attributes=trace_attributes,
                context=current_context,
            ) as span:
                span.set_attribute(
                    SpanAttributes.MESSAGING_OPERATION, MessageAction.PUBLISH
                )
                result = await call_next(msg, *args, headers=headers, **kwargs)

        except Exception as e:
            metrics_attributes[ERROR_TYPE] = type(e).__name__
            raise

        finally:
            duration = time.perf_counter() - start_time
            self._metrics.observe_publish(metrics_attributes, duration, msg_count)

        for key, token in self._scope_tokens:
            fs_context.reset_local(key, token)

        return result

    async def consume_scope(
        self,
        call_next: "AsyncFuncAny",
        msg: "StreamMessage[Any]",
    ) -> Any:
        if (provider := self.__settings_provider) is None:
            return await call_next(msg)

        if _is_batch_message(msg):
            links = _get_msg_links(msg)
            current_context = Context()
        else:
            links = None
            current_context = _TRACE_PROPAGATOR.extract(msg.headers)

        destination_name = provider.get_consume_destination_name(msg)
        trace_attributes = provider.get_consume_attrs_from_message(msg)
        metrics_attributes = {
            SpanAttributes.MESSAGING_SYSTEM: provider.messaging_system,
            MESSAGING_DESTINATION_PUBLISH_NAME: destination_name,
        }

        if not len(current_context):
            create_span = self._tracer.start_span(
                name=_create_span_name(destination_name, MessageAction.CREATE),
                kind=trace.SpanKind.CONSUMER,
                attributes=trace_attributes,
                links=links,
            )
            current_context = trace.set_span_in_context(create_span)
            create_span.end()

        self._origin_context = current_context
        start_time = time.perf_counter()

        try:
            with self._tracer.start_as_current_span(
                name=_create_span_name(destination_name, MessageAction.PROCESS),
                kind=trace.SpanKind.CONSUMER,
                context=current_context,
                attributes=trace_attributes,
                end_on_exit=False,
            ) as span:
                span.set_attribute(
                    SpanAttributes.MESSAGING_OPERATION, MessageAction.PROCESS
                )
                self._current_span = span

                self._scope_tokens.append(("span", fs_context.set_local("span", span)))
                self._scope_tokens.append(
                    ("baggage", fs_context.set_local("baggage", Baggage.from_msg(msg)))
                )

                new_context = trace.set_span_in_context(span, current_context)
                token = context.attach(new_context)
                result = await call_next(msg)
                context.detach(token)

        except Exception as e:
            metrics_attributes[ERROR_TYPE] = type(e).__name__
            raise

        finally:
            duration = time.perf_counter() - start_time
            msg_count = trace_attributes.get(
                SpanAttributes.MESSAGING_BATCH_MESSAGE_COUNT, 1
            )
            self._metrics.observe_consume(metrics_attributes, duration, msg_count)

        return result

    async def after_processed(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> Optional[bool]:
        if self._current_span and self._current_span.is_recording():
            self._current_span.end()
        return False


class TelemetryMiddleware:
    # NOTE: should it be class or function?
    __slots__ = (
        "_meter",
        "_metrics",
        "_settings_provider_factory",
        "_tracer",
    )

    def __init__(
        self,
        *,
        settings_provider_factory: Callable[
            [Any], Optional[TelemetrySettingsProvider[Any]]
        ],
        tracer_provider: Optional["TracerProvider"] = None,
        meter_provider: Optional["MeterProvider"] = None,
        meter: Optional["Meter"] = None,
        include_messages_counters: bool = False,
    ) -> None:
        self._tracer = _get_tracer(tracer_provider)
        self._meter = _get_meter(meter_provider, meter)
        self._metrics = _MetricsContainer(self._meter, include_messages_counters)
        self._settings_provider_factory = settings_provider_factory

    def __call__(self, msg: Optional[Any]) -> BaseMiddleware:
        return BaseTelemetryMiddleware(
            tracer=self._tracer,
            metrics_container=self._metrics,
            settings_provider_factory=self._settings_provider_factory,
            msg=msg,
        )


def _get_meter(
    meter_provider: Optional["MeterProvider"] = None,
    meter: Optional["Meter"] = None,
) -> "Meter":
    if meter is None:
        return metrics.get_meter(
            __name__,
            meter_provider=meter_provider,
            schema_url=OTEL_SCHEMA,
        )
    return meter


def _get_tracer(tracer_provider: Optional["TracerProvider"] = None) -> "Tracer":
    return trace.get_tracer(
        instrumenting_module_name=INSTRUMENTING_MODULE_NAME,
        instrumenting_library_version=INSTRUMENTING_LIBRARY_VERSION,
        tracer_provider=tracer_provider,
        schema_url=OTEL_SCHEMA,
    )


def _create_span_name(destination: str, action: str) -> str:
    return f"{destination} {action}"


def _is_batch_message(msg: "StreamMessage[Any]") -> bool:
    with_batch = baggage.get_baggage(
        WITH_BATCH, _BAGGAGE_PROPAGATOR.extract(msg.headers)
    )
    return bool(msg.batch_headers or with_batch)


def _get_msg_links(msg: "StreamMessage[Any]") -> List[Link]:
    if not msg.batch_headers:
        if (span := _get_span_from_headers(msg.headers)) is not None:
            return [Link(span.get_span_context())]
        else:
            return []

    links = {}
    counter: Dict[str, int] = defaultdict(lambda: 0)

    for headers in msg.batch_headers:
        if (correlation_id := headers.get("correlation_id")) is None:
            continue

        counter[correlation_id] += 1

        if (span := _get_span_from_headers(headers)) is None:
            continue

        attributes = _get_link_attributes(counter[correlation_id])

        links[correlation_id] = Link(
            span.get_span_context(),
            attributes=attributes,
        )

    return list(links.values())


def _get_span_from_headers(headers: "AnyDict") -> Optional[Span]:
    trace_context = _TRACE_PROPAGATOR.extract(headers)
    if not len(trace_context):
        return None

    return cast(
        "Optional[Span]",
        next(iter(trace_context.values())),
    )


def _get_link_attributes(message_count: int) -> "Attributes":
    if message_count <= 1:
        return {}
    return {
        SpanAttributes.MESSAGING_BATCH_MESSAGE_COUNT: message_count,
    }
