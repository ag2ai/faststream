import pytest

from faststream.kafka import KafkaBroker, KafkaRoute
from faststream.kafka.configs.broker import KafkaBrokerConfig


@pytest.mark.kafka()
class TestBrokerClientRack:
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


@pytest.mark.kafka()
class TestSubscriberClientRack:
    def test_subscriber_passes_client_rack(self) -> None:
        broker = KafkaBroker()

        sub = broker.subscriber(
            "test-topic",
            group_id="test-group",
            client_rack="sub-rack",
        )

        assert sub._connection_args["client_rack"] == "sub-rack"

    def test_subscriber_omits_client_rack_when_not_set(self) -> None:
        broker = KafkaBroker()

        sub = broker.subscriber("test-topic", group_id="test-group")

        # An unset subscriber value means "no override": client_rack must be
        # absent so the broker-level default is used instead.
        assert "client_rack" not in sub._connection_args

    def test_subscriber_client_rack_overrides_broker_default(self) -> None:
        broker = KafkaBroker(client_rack="broker-rack")

        sub = broker.subscriber(
            "test-topic",
            group_id="test-group",
            client_rack="sub-rack",
        )

        # The broker-level default still feeds the consumer builder...
        builder = broker.config.broker_config.builder
        assert builder.keywords["client_rack"] == "broker-rack"
        # ...while the subscriber override is forwarded at consume time and
        # takes precedence over the builder default.
        assert sub._connection_args["client_rack"] == "sub-rack"

    def test_route_accepts_client_rack(self) -> None:
        async def handler(msg: str) -> None: ...

        route = KafkaRoute(
            handler,
            "test-topic",
            group_id="test-group",
            client_rack="route-rack",
        )

        assert route.kwargs["client_rack"] == "route-rack"
