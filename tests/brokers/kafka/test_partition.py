import pytest

from faststream.kafka import TopicPartition


@pytest.mark.kafka()
def test_topic_partition_stores_declaration_settings() -> None:
    assert TopicPartition.__module__.startswith("faststream.")
    partition = TopicPartition("topic", 1, declare=False)

    assert (partition.topic, partition.partition, partition.declare) == (
        "topic",
        1,
        False,
    )
