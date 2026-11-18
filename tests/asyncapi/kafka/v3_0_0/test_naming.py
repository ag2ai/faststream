import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.kafka import KafkaBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.kafka()
class TestNaming(NamingTestCase):
    broker_class = KafkaBroker

    def test_base(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_pattern(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.subscriber(pattern="events.*")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_pattern_template_renders_as_declared(
        self,
        snapshot_json: SnapshotAssertion,
    ) -> None:
        broker = self.broker_class()

        @broker.subscriber(pattern="events.{version}")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
