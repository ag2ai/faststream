import pytest

from faststream import TestApp
from tests.marks import require_aiokafka


@pytest.mark.asyncio()
@require_aiokafka
async def test_broker_kafka() -> None:
    from docs.docs_src.getting_started.prometheus.kafka_custom_metric import (
        app,
        broker,
        registry,
    )
    from faststream.kafka import TestKafkaBroker

    expected_count_value = 10
    expected_custom_metric_name = "custom_counter"

    async with TestKafkaBroker(broker), TestApp(app):
        await broker.publish(expected_count_value, topic="test-topic")

        metrics = registry.collect()
        metrics_values = {
            m.name: m.samples[0].value for m in metrics if len(m.samples) > 0
        }
        custom_metric_value = metrics_values.get(expected_custom_metric_name)

        assert custom_metric_value is not None
        assert custom_metric_value == expected_count_value
