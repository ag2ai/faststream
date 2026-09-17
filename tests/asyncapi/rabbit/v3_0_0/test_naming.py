import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.rabbit import RabbitBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.rabbit()
class TestNaming(NamingTestCase):
    broker_class: type[RabbitBroker] = RabbitBroker

    def test_subscriber_with_exchange(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.subscriber("test", "exchange")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_publisher_with_exchange(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.publisher("test", "exchange")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_base(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
