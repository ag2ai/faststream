from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.kafka import KafkaBroker, TopicPartition
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase


@pytest.mark.kafka()
class TestArguments(ArgumentsTestcase):
    broker_class = KafkaBroker

    def test_subscriber_bindings(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_subscriber_with_one_topic_partitions(
        self,
        snapshot_json: SnapshotAssertion,
    ) -> None:
        broker = self.broker_class()

        part1 = TopicPartition("topic_name", 1)
        part2 = TopicPartition("topic_name", 2)

        @broker.subscriber(partitions=[part1, part2])
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json

    def test_subscriber_with_multi_topics_partitions(
        self,
        snapshot_json: SnapshotAssertion,
    ) -> None:
        broker = self.broker_class()

        part1 = TopicPartition("topic_name1", 1)
        part2 = TopicPartition("topic_name2", 2)

        @broker.subscriber(partitions=[part1, part2])
        async def handle(msg: Any) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json
