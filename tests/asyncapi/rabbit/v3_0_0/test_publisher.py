from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


@pytest.mark.rabbit()
class TestArguments(PublisherTestcase):
    broker_class = RabbitBroker

    def test_just_exchange(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class("amqp://guest:guest@localhost:5672/vhost")

        @broker.publisher(exchange="test-ex")
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_publisher_bindings(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.publisher(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.TOPIC),
        )
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_useless_queue_bindings(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.publisher(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.FANOUT),
        )
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_reusable_exchange(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class("amqp://guest:guest@localhost:5672/vhost")

        @broker.publisher(exchange="test-ex", routing_key="key1")
        @broker.publisher(exchange="test-ex", routing_key="key2", priority=10)
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
