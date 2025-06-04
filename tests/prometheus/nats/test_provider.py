import random
from types import SimpleNamespace

import pytest
from nats.aio.msg import Msg

from faststream.nats.prometheus.provider import (
    BatchNatsMetricsSettingsProvider,
    NatsMetricsSettingsProvider,
    settings_provider_factory,
)
from tests.prometheus.basic import LocalMetricsSettingsProviderTestcase

from .basic import BatchNatsPrometheusSettings, NatsPrometheusSettings


class LocalBaseNatsMetricsSettingsProviderTestcase(
    LocalMetricsSettingsProviderTestcase
):
    def test_get_publish_destination_name_from_cmd(self, queue: str) -> None:
        expected_destination_name = queue
        command = SimpleNamespace(destination=queue)

        provider = self.get_settings_provider()
        destination_name = provider.get_publish_destination_name_from_cmd(command)

        assert destination_name == expected_destination_name


class TestNatsMetricsSettingsProvider(
    NatsPrometheusSettings, LocalBaseNatsMetricsSettingsProviderTestcase
):
    def test_get_consume_attrs_from_message(self, queue: str) -> None:
        body = b"Hello"
        expected_attrs = {
            "destination_name": queue,
            "message_size": len(body),
            "messages_count": 1,
        }
        message = SimpleNamespace(body=body, raw_message=SimpleNamespace(subject=queue))

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs


class TestBatchNatsMetricsSettingsProvider(
    BatchNatsPrometheusSettings, LocalBaseNatsMetricsSettingsProviderTestcase
):
    def test_get_consume_attrs_from_message(self, queue: str) -> None:
        body = b"Hello"
        raw_messages = [
            SimpleNamespace(subject=queue) for _ in range(random.randint(a=2, b=10))
        ]

        expected_attrs = {
            "destination_name": raw_messages[0].subject,
            "message_size": len(body),
            "messages_count": len(raw_messages),
        }
        message = SimpleNamespace(body=body, raw_message=raw_messages)

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs


@pytest.mark.parametrize(
    ("msg", "expected_provider"),
    (
        pytest.param(
            (Msg(SimpleNamespace()), Msg(SimpleNamespace())),
            BatchNatsMetricsSettingsProvider(),
            id="message is sequence",
        ),
        pytest.param(
            Msg(
                SimpleNamespace(),
            ),
            NatsMetricsSettingsProvider(),
            id="single message",
        ),
        pytest.param(
            None,
            NatsMetricsSettingsProvider(),
            id="message is None",
        ),
        pytest.param(
            SimpleNamespace(),
            None,
            id="message is not Msg instance",
        ),
    ),
)
def test_settings_provider_factory(msg, expected_provider) -> None:
    provider = settings_provider_factory(msg)

    assert isinstance(provider, type(expected_provider))
