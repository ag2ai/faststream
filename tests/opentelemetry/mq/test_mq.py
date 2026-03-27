import pytest

from faststream.mq.opentelemetry import MQTelemetryMiddleware
from tests.brokers.mq.basic import MQTestcaseConfig
from tests.marks import require_ibmmq
from tests.opentelemetry.basic import LocalTelemetryTestcase


@require_ibmmq
@pytest.mark.connected()
@pytest.mark.mq()
@pytest.mark.asyncio()
class TestTelemetry(MQTestcaseConfig, LocalTelemetryTestcase):  # type: ignore[misc]
    messaging_system = "ibm_mq"
    include_messages_counters = False
    telemetry_middleware_class = MQTelemetryMiddleware
