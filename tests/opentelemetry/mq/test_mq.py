import pytest

from faststream.mq.opentelemetry import MQTelemetryMiddleware
from tests.brokers.mq.basic import MQMemoryTestcaseConfig
from tests.opentelemetry.basic import LocalTelemetryTestcase


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestTelemetry(MQMemoryTestcaseConfig, LocalTelemetryTestcase):  # type: ignore[misc]
    messaging_system = "ibm_mq"
    include_messages_counters = False
    telemetry_middleware_class = MQTelemetryMiddleware
