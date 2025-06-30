import random
from types import SimpleNamespace

import pytest

from faststream.confluent.prometheus.provider import (
    BatchConfluentMetricsSettingsProvider,
    ConfluentMetricsSettingsProvider,
    settings_provider_factory,
)
from tests.prometheus.basic import LocalMetricsSettingsProviderTestcase

from .basic import BatchConfluentPrometheusSettings, ConfluentPrometheusSettings


class LocalBaseConfluentMetricsSettingsProviderTestcase(
    LocalMetricsSettingsProviderTestcase,
):
    def test_get_publish_destination_name_from_cmd(self, queue: str) -> None:
        expected_destination_name = queue
        provider = self.get_settings_provider()
        command = SimpleNamespace(destination=queue)

        destination_name = provider.get_publish_destination_name_from_cmd(command)

        assert destination_name == expected_destination_name


class TestKafkaMetricsSettingsProvider(
    ConfluentPrometheusSettings, LocalBaseConfluentMetricsSettingsProviderTestcase
):
    def test_get_consume_attrs_from_message(self, queue: str) -> None:
        body = b"Hello"
        expected_attrs = {
            "destination_name": queue,
            "message_size": len(body),
            "messages_count": 1,
        }

        message = SimpleNamespace(
            body=body, raw_message=SimpleNamespace(topic=lambda: queue)
        )

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs


class TestBatchConfluentMetricsSettingsProvider(
    BatchConfluentPrometheusSettings, LocalBaseConfluentMetricsSettingsProviderTestcase
):
    def test_get_consume_attrs_from_message(self, queue: str) -> None:
        body = [b"Hi ", b"again, ", b"FastStream!"]
        message = SimpleNamespace(
            body=body,
            raw_message=[
                SimpleNamespace(topic=lambda: queue)
                for _ in range(random.randint(a=2, b=10))
            ],
        )
        expected_attrs = {
            "destination_name": message.raw_message[0].topic(),
            "message_size": len(bytearray().join(body)),
            "messages_count": len(message.raw_message),
        }

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs


@pytest.mark.parametrize(
    ("msg", "expected_provider"),
    (
        pytest.param(
            (SimpleNamespace(), SimpleNamespace()),
            BatchConfluentMetricsSettingsProvider(),
            id="batch message",
        ),
        pytest.param(
            SimpleNamespace(),
            ConfluentMetricsSettingsProvider(),
            id="single message",
        ),
        pytest.param(
            None,
            ConfluentMetricsSettingsProvider(),
            id="None message",
        ),
    ),
)
def test_settings_provider_factory(msg, expected_provider) -> None:
    provider = settings_provider_factory(msg)

    assert isinstance(provider, type(expected_provider))
