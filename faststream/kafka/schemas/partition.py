from typing import NamedTuple


class TopicPartition(NamedTuple):
    """A topic and partition pair, naming the assignment a Subscriber is declared with.

    FastStream's own rather than a re-export of `aiokafka.TopicPartition`, so the
    type is ours to grow at the same import path. Still a `NamedTuple` with the
    client library's two fields in the same order, because unpacking, indexing,
    ordering, hashing and equality against the client library's tuple are what the
    re-export gave users and none of them survive an ordinary class, or a third
    field. What the consumer is assigned is the client library's tuple, rebuilt
    where the assignment happens; this one is never handed to aiokafka.

    Separate from `faststream.confluent.TopicPartition`, which carries `offset`,
    `leader_epoch` and `metadata` — fields the aiokafka client has no equivalent
    for, and so a promise this signature must not make.
    """

    topic: str
    """A topic name."""

    partition: int
    """A partition id."""
