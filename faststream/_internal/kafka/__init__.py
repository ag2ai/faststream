"""Machinery shared by the two Kafka broker packages.

`faststream.kafka` (aiokafka) and `faststream.confluent` (confluent-kafka) are
independent -- neither imports the other -- so anything both need lives here
rather than in one of them or in a cross-broker module.
"""

from .keys import (
    extract_per_message_keys_and_bodies,
    key_for_index,
    realign_keys,
)

__all__ = (
    "extract_per_message_keys_and_bodies",
    "key_for_index",
    "realign_keys",
)
