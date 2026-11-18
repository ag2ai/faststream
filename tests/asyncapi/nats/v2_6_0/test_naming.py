import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.nats import NatsBroker
from tests.asyncapi.base.v2_6_0.naming import NamingTestCase


@pytest.mark.nats()
class TestNaming(NamingTestCase):
    broker_class = NatsBroker

    def test_base(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
