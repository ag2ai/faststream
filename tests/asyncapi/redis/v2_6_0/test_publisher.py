from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.redis import RedisBroker
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


@pytest.mark.redis()
class TestArguments(PublisherTestcase):
    broker_class = RedisBroker

    def test_channel_publisher(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_list_publisher(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.publisher(list="test")
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_stream_publisher(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.publisher(stream="test")
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
