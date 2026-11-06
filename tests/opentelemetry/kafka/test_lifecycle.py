import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from faststream import BaseMiddleware
from faststream.kafka import KafkaBroker, TestKafkaBroker
from faststream.kafka.opentelemetry import KafkaTelemetryMiddleware
from faststream.message import StreamMessage
from faststream.middlewares import AckPolicy


@pytest.mark.kafka()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "cleanup_timing", ("before_start", "during_processing", "after_processing")
)
@pytest.mark.parametrize("outcome", ("success", "error", "cancel"))
async def test_background_processing_span_lifetime(
    cleanup_timing: str, outcome: str
) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    started = asyncio.Event()
    release = asyncio.Event()
    tasks: list[asyncio.Future[Any]] = []
    spans: list[trace.Span] = []
    completed_at: list[int] = []

    class BackgroundMiddleware(BaseMiddleware):
        async def consume_scope(
            self,
            call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
            msg: StreamMessage[Any],
        ) -> None:
            tasks.append(asyncio.ensure_future(call_next(msg)))
            if cleanup_timing == "during_processing":
                await started.wait()
            elif cleanup_timing == "after_processing":
                await tasks[-1]

    broker = KafkaBroker(
        middlewares=(
            BackgroundMiddleware,
            KafkaTelemetryMiddleware(tracer_provider=provider),
        )
    )

    @broker.subscriber("telemetry-lifecycle", ack_policy=AckPolicy.MANUAL)
    async def handler(body: str) -> None:
        spans.append(trace.get_current_span())
        started.set()
        try:
            if cleanup_timing != "after_processing":
                await release.wait()
            if outcome == "error":
                raise ValueError
            if outcome == "cancel":
                raise asyncio.CancelledError
        finally:
            completed_at.append(time.time_ns())

    try:
        async with TestKafkaBroker(broker):
            if cleanup_timing == "after_processing" and outcome != "success":
                with pytest.raises(
                    ValueError if outcome == "error" else asyncio.CancelledError
                ):
                    await broker.publish("message", "telemetry-lifecycle")
            else:
                await broker.publish("message", "telemetry-lifecycle")
            await asyncio.wait_for(started.wait(), 2)
            if cleanup_timing != "after_processing":
                assert spans[0].is_recording()
                assert not [
                    s
                    for s in exporter.get_finished_spans()
                    if s.name.endswith(" process")
                ]
                release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        process_spans = [
            s for s in exporter.get_finished_spans() if s.name.endswith(" process")
        ]
        assert len(process_spans) == 1
        span = process_spans[0]
        assert span.end_time is not None
        assert span.end_time >= completed_at[0]
        assert not spans[0].is_recording()
        if outcome == "error":
            assert span.status.status_code is trace.StatusCode.ERROR
            assert any(event.name == "exception" for event in span.events)
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        provider.shutdown()


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_overlapping_background_spans_keep_trace_context() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    release = asyncio.Event()
    both_started = asyncio.Event()
    tasks: list[asyncio.Future[Any]] = []
    handler_spans: list[trace.Span] = []

    class BackgroundMiddleware(BaseMiddleware):
        async def consume_scope(
            self,
            call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
            msg: StreamMessage[Any],
        ) -> None:
            tasks.append(asyncio.ensure_future(call_next(msg)))

    broker = KafkaBroker(
        middlewares=(
            BackgroundMiddleware,
            KafkaTelemetryMiddleware(tracer_provider=provider),
        )
    )

    @broker.subscriber("telemetry-overlap", ack_policy=AckPolicy.MANUAL)
    async def handler(body: str) -> None:
        span = trace.get_current_span()
        handler_spans.append(span)
        if len(handler_spans) == 2:
            both_started.set()
        await release.wait()
        assert trace.get_current_span() is span
        await broker.publish(body, "telemetry-output")

    try:
        async with TestKafkaBroker(broker):
            await broker.publish("first", "telemetry-overlap")
            await broker.publish("second", "telemetry-overlap")
            await asyncio.wait_for(both_started.wait(), 2)
            assert all(span.is_recording() for span in handler_spans)
            release.set()
            await asyncio.gather(*tasks)
        spans = exporter.get_finished_spans()
        process_spans = [
            span for span in spans if span.name == "telemetry-overlap process"
        ]
        publish_spans = [
            span for span in spans if span.name == "telemetry-output publish"
        ]
        assert len(process_spans) == len(publish_spans) == 2
        assert len({span.context.trace_id for span in process_spans}) == 2
        for process_span in process_spans:
            children = [
                span
                for span in publish_spans
                if span.context.trace_id == process_span.context.trace_id
            ]
            assert len(children) == 1
            assert process_span.end_time is not None
            assert children[0].end_time is not None
            assert process_span.end_time >= children[0].end_time
        assert all(not span.is_recording() for span in handler_spans)
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        provider.shutdown()
