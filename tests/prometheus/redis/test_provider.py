from types import SimpleNamespace

import pytest

from faststream.redis.message import (
    BatchListMessage,
    BatchStreamMessage,
    DefaultListMessage,
    DefaultStreamMessage,
    PubSubMessage,
)
from faststream.redis.prometheus.provider import (
    BatchRedisMetricsSettingsProvider,
    RedisMetricsSettingsProvider,
    settings_provider_factory,
)
from tests.prometheus.basic import LocalMetricsSettingsProviderTestcase

from .basic import BatchRedisPrometheusSettings, RedisPrometheusSettings


class LocalBaseRedisMetricsSettingsProviderTestcase(
    LocalMetricsSettingsProviderTestcase
):
    def test_get_publish_destination_name_from_cmd(self, queue: str) -> None:
        expected_destination_name = queue
        provider = self.get_settings_provider()
        command = SimpleNamespace(destination=queue)

        destination_name = provider.get_publish_destination_name_from_cmd(command)

        assert destination_name == expected_destination_name


class TestRedisMetricsSettingsProvider(
    RedisPrometheusSettings, LocalBaseRedisMetricsSettingsProviderTestcase
):
    @pytest.mark.parametrize(
        "destination",
        (
            pytest.param("channel", id="destination is channel"),
            pytest.param("list", id="destination is list"),
            pytest.param("stream", id="destination is stream"),
            pytest.param("", id="destination is blank"),
        ),
    )
    def test_get_consume_attrs_from_message(self, queue: str, destination: str) -> None:
        body = b"Hello"
        expected_attrs = {
            "destination_name": queue if destination else "",
            "message_size": len(body),
            "messages_count": 1,
        }

        raw_message = {"data": body}
        if destination:
            raw_message[destination] = queue

        message = SimpleNamespace(body=body, raw_message=raw_message)

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs


class TestBatchRedisMetricsSettingsProvider(
    BatchRedisPrometheusSettings,
    LocalBaseRedisMetricsSettingsProviderTestcase,
):
    @pytest.mark.parametrize(
        "destination",
        (
            pytest.param("channel", id="destination is channel"),
            pytest.param("list", id="destination is list"),
            pytest.param("stream", id="destination is stream"),
            pytest.param("", id="destination is blank"),
        ),
    )
    def test_get_consume_attrs_from_message(self, queue: str, destination: str) -> None:
        decoded_body = ["Hi ", "again, ", "FastStream!"]
        body = str(decoded_body).encode()

        expected_attrs = {
            "destination_name": queue if destination else "",
            "message_size": len(body),
            "messages_count": len(decoded_body),
        }

        raw_message = {"data": decoded_body}

        if destination:
            raw_message[destination] = queue

        message = SimpleNamespace(
            body=body,
            raw_message=raw_message,
        )

        provider = self.get_settings_provider()
        attrs = provider.get_consume_attrs_from_message(message)

        assert attrs == expected_attrs


@pytest.mark.parametrize(
    ("msg", "expected_provider"),
    (
        pytest.param(
            PubSubMessage(
                type="message",
                channel="test-channel",
                data=b"",
                pattern=None,
            ),
            RedisMetricsSettingsProvider(),
            id="PubSub message",
        ),
        pytest.param(
            DefaultListMessage(type="list", channel="test-list", data=b""),
            RedisMetricsSettingsProvider(),
            id="Single List message",
        ),
        pytest.param(
            BatchListMessage(type="blist", channel="test-list", data=[b"", b""]),
            BatchRedisMetricsSettingsProvider(),
            id="Batch List message",
        ),
        pytest.param(
            DefaultStreamMessage(
                type="stream",
                channel="test-stream",
                data=b"",
                message_ids=[],
            ),
            RedisMetricsSettingsProvider(),
            id="Single Stream message",
        ),
        pytest.param(
            BatchStreamMessage(
                type="bstream",
                channel="test-stream",
                data=[{b"": b""}, {b"": b""}],
                message_ids=[],
            ),
            BatchRedisMetricsSettingsProvider(),
            id="Batch Stream message",
        ),
    ),
)
def test_settings_provider_factory(msg, expected_provider) -> None:
    provider = settings_provider_factory(msg)

    assert isinstance(provider, type(expected_provider))
