from types import SimpleNamespace

import pytest

from tests.prometheus.basic import LocalMetricsSettingsProviderTestcase

from .basic import SQSPrometheusSettings


@pytest.mark.sqs()
class TestSQSMetricsSettingsProvider(
    SQSPrometheusSettings,
    LocalMetricsSettingsProviderTestcase,
):
    def test_get_publish_destination_name_from_cmd(self, queue: str) -> None:
        provider = self.get_settings_provider()
        command = SimpleNamespace(destination=queue)

        destination_name = provider.get_publish_destination_name_from_cmd(command)

        assert destination_name == queue

    def test_get_consume_attrs_from_message(self, queue: str) -> None:
        body = b"Hello"
        # SQS reports the consume destination by queue URL.
        queue_url = f"http://localhost:4566/000000000000/{queue}"
        expected_attrs = {
            "destination_name": queue_url,
            "message_size": len(body),
            "messages_count": 1,
        }

        message = SimpleNamespace(body=body, queue_url=queue_url)

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs
