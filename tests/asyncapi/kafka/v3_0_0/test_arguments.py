from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.kafka import KafkaBroker
from tests.asyncapi.base.v3_0_0.arguments import ArgumentsTestcase


@pytest.mark.kafka()
class TestArguments(ArgumentsTestcase):
    broker_class = KafkaBroker

    def test_subscriber_bindings(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
