import pytest

from faststream.kafka import KafkaBroker
from faststream.kafka.configs.broker import KafkaBrokerConfig


@pytest.mark.kafka()
class TestClientRack:
    def test_broker_config_stores_client_rack(self) -> None:
        config = KafkaBrokerConfig(client_rack="us-east-1a")

        assert config.client_rack == "us-east-1a"

    def test_broker_config_client_rack_defaults_to_none(self) -> None:
        assert KafkaBrokerConfig().client_rack is None

    def test_broker_forwards_client_rack_to_consumer(self) -> None:
        broker = KafkaBroker(client_rack="us-east-1a")

        broker_config = broker.config.broker_config
        assert broker_config.client_rack == "us-east-1a"
        assert broker_config.builder.keywords["client_rack"] == "us-east-1a"

    def test_broker_omits_client_rack_when_not_set(self) -> None:
        broker = KafkaBroker()

        # When client_rack is not provided it must not be passed to the
        # consumer at all, so older aiokafka versions remain unaffected.
        builder = broker.config.broker_config.builder
        assert "client_rack" not in builder.keywords
