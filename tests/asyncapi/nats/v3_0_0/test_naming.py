import pytest
from nats.js.api import ConsumerConfig
from syrupy.assertion import SnapshotAssertion

from faststream.nats import JStream, NatsBroker, PullSub
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.nats()
class TestNaming(NamingTestCase):
    broker_class = NatsBroker

    def test_filter_subjects_without_subject(
        self, snapshot_json: SnapshotAssertion
    ) -> None:
        """A JetStream consumer may address a stream through `filter_subjects` and no `subject`."""
        broker = self.broker_class()

        @broker.subscriber(
            stream=JStream("stream"),
            pull_sub=PullSub(),
            durable="durable",
            config=ConsumerConfig(filter_subjects=["logs.{level}"]),
        )
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_multiple_filter_subjects_without_subject(
        self, snapshot_json: SnapshotAssertion
    ) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            stream=JStream("stream"),
            pull_sub=PullSub(),
            durable="durable",
            config=ConsumerConfig(filter_subjects=["logs.info", "logs.error"]),
        )
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_base(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
