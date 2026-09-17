import pytest
from aiokafka import TopicPartition as AIOKafkaTopicPartition

from faststream.kafka import TopicPartition


@pytest.mark.kafka()
def test_topic_partition_is_faststreams_own_and_still_the_client_library_tuple() -> None:
    assert TopicPartition.__module__.startswith("faststream.")
    # what the consumer is assigned is aiokafka's tuple; user code compares the two
    assert TopicPartition("topic", 1) == AIOKafkaTopicPartition("topic", 1)
