from types import SimpleNamespace

import pytest

from faststream.sqs.prometheus.provider import (
    BatchSQSMetricsSettingsProvider,
    SQSMetricsSettingsProvider,
    settings_provider_factory,
)
from tests.prometheus.basic import LocalMetricsSettingsProviderTestcase

from .basic import BatchSQSPrometheusSettings, SQSPrometheusSettings


class LocalBaseSQSMetricsSettingsProviderTestcase(
    LocalMetricsSettingsProviderTestcase,
):
    def test_get_publish_destination_name_from_cmd(self, queue: str) -> None:
        provider = self.get_settings_provider()
        command = SimpleNamespace(destination=queue)

        destination_name = provider.get_publish_destination_name_from_cmd(command)

        assert destination_name == queue


@pytest.mark.sqs()
class TestSQSMetricsSettingsProvider(
    SQSPrometheusSettings,
    LocalBaseSQSMetricsSettingsProviderTestcase,
):
    def test_get_consume_attrs_from_message(self, queue: str) -> None:
        body = b"Hello"
        # SQS reports the consume destination by queue URL.
        queue_url = f"http://localhost:9324/000000000000/{queue}"
        expected_attrs = {
            "destination_name": queue_url,
            "message_size": len(body),
            "messages_count": 1,
        }

        message = SimpleNamespace(body=body, queue_url=queue_url)

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs


@pytest.mark.sqs()
class TestBatchSQSMetricsSettingsProvider(
    BatchSQSPrometheusSettings,
    LocalBaseSQSMetricsSettingsProviderTestcase,
):
    def test_get_consume_attrs_from_message(self, queue: str) -> None:
        bodies = [b"Hi ", b"again, ", b"FastStream!"]
        queue_url = f"http://localhost:9324/000000000000/{queue}"
        message = SimpleNamespace(
            body=bodies,
            queue_url=queue_url,
            raw_message=[SimpleNamespace() for _ in bodies],
        )
        expected_attrs = {
            "destination_name": queue_url,
            "message_size": len(bytearray().join(bodies)),
            "messages_count": len(message.raw_message),
        }

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs


@pytest.mark.sqs()
@pytest.mark.parametrize(
    ("msg", "expected_provider"),
    (
        pytest.param(
            [{"Body": "1"}, {"Body": "2"}],
            BatchSQSMetricsSettingsProvider(),
            id="batch message",
        ),
        pytest.param(
            {"Body": "1"},
            SQSMetricsSettingsProvider(),
            id="single message",
        ),
        pytest.param(
            None,
            SQSMetricsSettingsProvider(),
            id="None message",
        ),
    ),
)
def test_settings_provider_factory(msg, expected_provider) -> None:
    provider = settings_provider_factory(msg)

    assert isinstance(provider, type(expected_provider))
