import pytest

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase


@pytest.mark.rabbit()
class TestArguments(ArgumentsTestcase):
    broker_class = RabbitBroker

    def test_subscriber_bindings(self, snapshot_json) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.TOPIC),
        )
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_subscriber_fanout_bindings(self, snapshot_json) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.FANOUT),
        )
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_subscriber_headers_bindings(self, snapshot_json) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.HEADERS),
        )
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        assert schema == snapshot_json

    def test_subscriber_xdelay_bindings(self, snapshot_json) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.X_DELAYED_MESSAGE),
        )
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_subscriber_consistent_hash_bindings(self, snapshot_json) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.X_CONSISTENT_HASH),
        )
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        assert schema == snapshot_json

    def test_subscriber_modules_hash_bindings(self, snapshot_json) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.X_MODULUS_HASH),
        )
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
