import re

import pytest
from opentelemetry.semconv.trace import SpanAttributes as SpanAttr

from faststream.mq.opentelemetry import MQTelemetryMiddleware
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME
from tests.brokers.mq.basic import MQTestcaseConfig
from tests.marks import require_ibmmq
from tests.opentelemetry.basic import LocalTelemetryTestcase

MQ_ID_RE = re.compile(r"^[0-9a-f]{48}$")


@require_ibmmq
@pytest.mark.connected()
@pytest.mark.mq()
@pytest.mark.asyncio()
class TestTelemetry(MQTestcaseConfig, LocalTelemetryTestcase):  # type: ignore[misc]
    messaging_system = "ibm_mq"
    include_messages_counters = False
    telemetry_middleware_class = MQTelemetryMiddleware

    def assert_span(
        self,
        span,
        action: str,
        queue: str,
        msg: str,
        parent_span_id: str | None = None,
    ) -> None:
        attrs = span.attributes or {}
        assert attrs[SpanAttr.MESSAGING_SYSTEM] == self.messaging_system

        conversation_id = attrs.get(SpanAttr.MESSAGING_MESSAGE_CONVERSATION_ID)
        if conversation_id is not None:
            assert MQ_ID_RE.fullmatch(conversation_id)

        if span.kind.name == "PRODUCER" and action in {"create", "publish"}:
            assert attrs[SpanAttr.MESSAGING_DESTINATION_NAME] == queue

        if span.kind.name == "CONSUMER" and action in {"create", "process"}:
            assert attrs[MESSAGING_DESTINATION_PUBLISH_NAME] == queue
            assert MQ_ID_RE.fullmatch(attrs[SpanAttr.MESSAGING_MESSAGE_ID])

        if action == "process":
            assert attrs[SpanAttr.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES] == len(msg)
            assert attrs[SpanAttr.MESSAGING_OPERATION] == action

        if action == "publish":
            assert attrs[SpanAttr.MESSAGING_OPERATION] == action

        if parent_span_id:
            assert span.parent is not None
            assert span.parent.span_id == parent_span_id
