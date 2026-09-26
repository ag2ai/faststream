from pathlib import Path
from uuid import uuid4

import pytest
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, values
from prometheus_client.parser import text_string_to_metric_families
from starlette.testclient import TestClient

from tests.marks import require_aiokafka

RECEIVED_TOTAL = "faststream_received_messages_total"


async def handle(_: str) -> None: ...


def received_totals(metrics_text: str, handler: str) -> list[float]:
    return [
        sample.value
        for family in text_string_to_metric_families(metrics_text)
        for sample in family.samples
        if sample.name == RECEIVED_TOTAL and sample.labels.get("handler") == handler
    ]


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_single_process_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    from docs.docs_src.getting_started.prometheus.kafka_multiprocess import (
        app,
        broker,
    )
    from faststream.kafka import TestKafkaBroker

    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)

    # to keep the counter value exact during subsequent runs
    topic = f"single-process-{uuid4().hex}"
    broker.subscriber(topic)(handle)

    async with TestKafkaBroker(broker) as br:
        await br.publish("Hi!", topic)

        with TestClient(app) as client:
            response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == CONTENT_TYPE_LATEST
    assert received_totals(response.text, topic) == [1.0]


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_multiprocess_metrics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from docs.docs_src.getting_started.prometheus.kafka_multiprocess import (
        app,
        broker,
    )
    from faststream.kafka import KafkaBroker, TestKafkaBroker
    from faststream.kafka.prometheus import KafkaPrometheusMiddleware

    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))

    # to prevent caching during subsequent runs
    topic = f"multiprocess-{uuid4().hex}"
    broker.subscriber(topic)(handle)

    other_broker = KafkaBroker(
        middlewares=(
            KafkaPrometheusMiddleware(
                registry=CollectorRegistry(),
                app_name="your-app-name",
            ),
        ),
    )
    other_broker.subscriber(topic)(handle)

    for pid, worker_broker in ((1, broker), (2, other_broker)):
        value_class = values.MultiProcessValue(lambda pid=pid: pid)  # type: ignore[no-untyped-call]
        monkeypatch.setattr(values, "ValueClass", value_class)

        async with TestKafkaBroker(worker_broker) as br:
            await br.publish("Hi!", topic)

    async with TestKafkaBroker(broker):
        with TestClient(app) as client:
            response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == CONTENT_TYPE_LATEST
    assert received_totals(response.text, topic) == [2.0]
