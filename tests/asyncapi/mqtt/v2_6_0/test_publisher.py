from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


@pytest.mark.mqtt()
class TestArguments(PublisherTestcase):
    broker_class = MQTTBroker

    def test_publisher_bindings(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
