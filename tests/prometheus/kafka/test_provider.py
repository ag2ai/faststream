import random
from types import SimpleNamespace

import pytest

from faststream._internal.kafka import TOMBSTONE
from faststream.kafka.prometheus.provider import (
    BatchKafkaMetricsSettingsProvider,
    KafkaMetricsSettingsProvider,
    settings_provider_factory,
)
from tests.prometheus.basic import LocalMetricsSettingsProviderTestcase

from .basic import BatchKafkaPrometheusSettings, KafkaPrometheusSettings


class LocalBaseKafkaMetricsSettingsProviderTestcase(
    LocalMetricsSettingsProviderTestcase,
):
    def test_get_publish_destination_name_from_cmd(self, queue: str) -> None:
        expected_destination_name = queue
        provider = self.get_settings_provider()
        command = SimpleNamespace(destination=queue)

        destination_name = provider.get_publish_destination_name_from_cmd(command)

        assert destination_name == expected_destination_name


@pytest.mark.kafka()
class TestKafkaMetricsSettingsProvider(
    KafkaPrometheusSettings,
    LocalBaseKafkaMetricsSettingsProviderTestcase,
):
    def test_get_consume_attrs_from_message(self, queue: str) -> None:
        body = b"Hello"
        expected_attrs = {
            "destination_name": queue,
            "message_size": len(body),
            "messages_count": 1,
        }

        message = SimpleNamespace(body=body, raw_message=SimpleNamespace(topic=queue))

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs

    def test_get_consume_attrs_from_a_tombstone(self, queue: str) -> None:
        message = SimpleNamespace(
            body=TOMBSTONE,
            raw_message=SimpleNamespace(topic=queue),
        )

        attrs = self.get_settings_provider().get_consume_attrs_from_message(message)

        assert attrs == {
            "destination_name": queue,
            "message_size": 0,
            "messages_count": 1,
        }


@pytest.mark.kafka()
class TestBatchKafkaMetricsSettingsProvider(
    BatchKafkaPrometheusSettings,
    LocalBaseKafkaMetricsSettingsProviderTestcase,
):
    def test_get_consume_attrs_from_message(self, queue: str) -> None:
        body = [b"Hi ", b"again, ", b"FastStream!"]
        message = SimpleNamespace(
            body=body,
            raw_message=[
                SimpleNamespace(topic=queue) for _ in range(random.randint(a=2, b=10))
            ],
        )
        expected_attrs = {
            "destination_name": message.raw_message[0].topic,
            "message_size": len(bytearray().join(body)),
            "messages_count": len(message.raw_message),
        }

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs

    def test_get_consume_attrs_from_a_batch_holding_a_tombstone(
        self,
        queue: str,
    ) -> None:
        body = [b"Hi", TOMBSTONE]
        message = SimpleNamespace(
            body=body,
            raw_message=[SimpleNamespace(topic=queue) for _ in body],
        )

        attrs = self.get_settings_provider().get_consume_attrs_from_message(message)

        assert attrs == {
            "destination_name": queue,
            "message_size": 2,
            "messages_count": 2,
        }


@pytest.mark.kafka()
@pytest.mark.parametrize(
    ("msg", "expected_provider"),
    (
        pytest.param(
            (SimpleNamespace(), SimpleNamespace()),
            BatchKafkaMetricsSettingsProvider(),
            id="batch message",
        ),
        pytest.param(
            SimpleNamespace(),
            KafkaMetricsSettingsProvider(),
            id="single message",
        ),
        pytest.param(
            None,
            KafkaMetricsSettingsProvider(),
            id="None message",
        ),
    ),
)
def test_settings_provider_factory(msg, expected_provider) -> None:
    provider = settings_provider_factory(msg)

    assert isinstance(provider, type(expected_provider))
