import pytest
from aiokafka import TopicPartition as AIOKafkaTopicPartition

from faststream.kafka import TopicPartition


@pytest.mark.kafka()
def test_topic_partition_is_faststreams_own_and_still_the_client_library_tuple() -> None:
    assert TopicPartition.__module__.startswith("faststream.")
    # what the consumer is assigned is aiokafka's tuple; user code compares the two
    assert TopicPartition("topic", 1) == AIOKafkaTopicPartition("topic", 1)


@pytest.mark.kafka()
def test_topic_partition_keeps_two_field_tuple_operations() -> None:
    partition = TopicPartition("topic", 1, declare=False)

    assert tuple(partition) == ("topic", 1)
    assert len(partition) == 2
    assert partition[0] == "topic"
    assert partition[1:] == (1,)
    assert hash(partition) == hash(("topic", 1))


@pytest.mark.kafka()
def test_topic_partition_supports_positional_pattern_matching() -> None:
    partition = TopicPartition("topic", 1)

    match partition:
        case TopicPartition(topic, number):
            result = (topic, number)

    assert result == ("topic", 1)
