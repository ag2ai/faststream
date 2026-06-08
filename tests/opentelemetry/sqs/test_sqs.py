from typing import Any

import pytest
from dirty_equals import IsUUID
from opentelemetry.sdk.trace import Span
from opentelemetry.semconv.trace import SpanAttributes as SpanAttr
from opentelemetry.trace import SpanKind

from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME
from faststream.opentelemetry.middleware import MessageAction as Action
from faststream.sqs import SQSBroker
from faststream.sqs.opentelemetry import SQSTelemetryMiddleware
from tests.brokers.sqs.basic import SQSTestcaseConfig
from tests.brokers.sqs.test_consume import TestConsume as ConsumeCase
from tests.brokers.sqs.test_publish import TestPublish as PublishCase
from tests.opentelemetry.basic import LocalTelemetryTestcase


@pytest.mark.sqs()
@pytest.mark.connected()
class TestTelemetry(SQSTestcaseConfig, LocalTelemetryTestcase):  # type: ignore[misc]
    messaging_system = "aws_sqs"
    include_messages_counters = True
    telemetry_middleware_class = SQSTelemetryMiddleware

    def assert_span(
        self,
        span: Span,
        action: str,
        queue: str,
        msg: str,
        parent_span_id: str | None = None,
    ) -> None:
        # SQS reports the publish side by queue *name* but the consume side by
        # queue *URL* (``.../<account>/<queue>``); the shared base assumes the
        # queue name on both sides, so the destination assertions are relaxed
        # to ``endswith`` for consumer spans.
        attrs = span.attributes or {}
        assert attrs[SpanAttr.MESSAGING_SYSTEM] == self.messaging_system, attrs[
            SpanAttr.MESSAGING_SYSTEM
        ]
        assert attrs[SpanAttr.MESSAGING_MESSAGE_CONVERSATION_ID] == IsUUID, attrs[
            SpanAttr.MESSAGING_MESSAGE_CONVERSATION_ID
        ]
        assert span.kind in {SpanKind.CONSUMER, SpanKind.PRODUCER}, span.kind

        if span.kind == SpanKind.PRODUCER and action in {Action.CREATE, Action.PUBLISH}:
            assert span.name == f"{queue} {action}", span.name
            assert attrs[SpanAttr.MESSAGING_DESTINATION_NAME] == queue, attrs[
                SpanAttr.MESSAGING_DESTINATION_NAME
            ]

        if span.kind == SpanKind.CONSUMER and action in {Action.CREATE, Action.PROCESS}:
            assert span.name.endswith(f"{queue} {action}"), span.name
            assert str(attrs[MESSAGING_DESTINATION_PUBLISH_NAME]).endswith(queue), attrs[
                MESSAGING_DESTINATION_PUBLISH_NAME
            ]
            assert attrs[SpanAttr.MESSAGING_MESSAGE_ID] == IsUUID, attrs[
                SpanAttr.MESSAGING_MESSAGE_ID
            ]

        if action == Action.PROCESS:
            assert attrs[SpanAttr.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES] == len(msg), (
                attrs[SpanAttr.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES]
            )
            assert attrs[SpanAttr.MESSAGING_OPERATION] == action, attrs[
                SpanAttr.MESSAGING_OPERATION
            ]

        if action == Action.PUBLISH:
            assert attrs[SpanAttr.MESSAGING_OPERATION] == action, attrs[
                SpanAttr.MESSAGING_OPERATION
            ]

        if parent_span_id:
            assert span.parent
            assert span.parent.span_id == parent_span_id, span.parent.span_id


@pytest.mark.sqs()
@pytest.mark.connected()
class TestPublishWithTelemetry(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> SQSBroker:
        broker = super().get_broker(apply_types=apply_types, **kwargs)
        broker.add_middleware(SQSTelemetryMiddleware())
        return broker


@pytest.mark.sqs()
@pytest.mark.connected()
class TestConsumeWithTelemetry(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> SQSBroker:
        broker = super().get_broker(apply_types=apply_types, **kwargs)
        broker.add_middleware(SQSTelemetryMiddleware())
        return broker
