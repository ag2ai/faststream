from types import SimpleNamespace
from typing import Union

import pytest

from tests.prometheus.basic import LocalMetricsSettingsProviderTestcase

from .basic import RabbitPrometheusSettings


class TestRabbitMetricsSettingsProvider(
    RabbitPrometheusSettings,
    LocalMetricsSettingsProviderTestcase,
):
    @pytest.mark.parametrize(
        "exchange",
        (
            pytest.param("my_exchange", id="with exchange"),
            pytest.param(None, id="without exchange"),
        ),
    )
    def test_get_consume_attrs_from_message(
        self,
        exchange: Union[str, None],
        queue: str,
    ) -> None:
        body = b"Hello"
        expected_attrs = {
            "destination_name": f"{exchange or 'default'}.{queue}",
            "message_size": len(body),
            "messages_count": 1,
        }
        message = SimpleNamespace(
            body=body, raw_message=SimpleNamespace(exchange=exchange, routing_key=queue)
        )

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs

    @pytest.mark.parametrize(
        "exchange",
        (
            pytest.param("my_exchange", id="with exchange"),
            pytest.param(None, id="without exchange"),
        ),
    )
    def test_get_publish_destination_name_from_cmd(
        self,
        exchange: Union[str, None],
        queue: str,
    ) -> None:
        expected_destination_name = f"{exchange or 'default'}.{queue}"
        command = SimpleNamespace(
            exchange=SimpleNamespace(name=exchange), destination=queue
        )

        provider = self.get_settings_provider()
        destination_name = provider.get_publish_destination_name_from_cmd(command)

        assert destination_name == expected_destination_name
