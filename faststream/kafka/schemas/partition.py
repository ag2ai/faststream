from collections.abc import Iterator
from typing import overload


class TopicPartition:
    """A topic and partition pair, naming the assignment a Subscriber is declared with.

    FastStream's own rather than a re-export of `aiokafka.TopicPartition`, so the
    type is ours to grow at the same import path. Still two-field-shaped:
    unpacking, indexing, hashing and equality against the client library's
    tuple compare on ``(topic, partition)`` only. `declare` rides alongside
    because a third field would break those comparisons. What the consumer is
    assigned is the client library's tuple, rebuilt where the assignment
    happens; this one is never handed to aiokafka.

    Separate from `faststream.confluent.TopicPartition`, which carries `offset`,
    `leader_epoch` and `metadata` — fields the aiokafka client has no equivalent
    for, and so a promise this signature must not make.
    """

    __slots__ = (
        "declare",
        "partition",
        "topic",
    )
    __match_args__ = ("topic", "partition")

    def __init__(
        self,
        topic: str,
        partition: int,
        *,
        declare: bool = True,
    ) -> None:
        """Initialize the Kafka topic partition.

        Args:
            topic: Kafka topic name.
            partition: Partition number to assign.
            declare: Whether to create the topic automatically or just connect to it.
                Missing topics are not created and their absence is not reported,
                so set it to `False` for topics provisioned by someone else.
        """
        self.topic = topic
        self.partition = partition
        self.declare = declare

    def __iter__(self) -> Iterator[str | int]:
        yield self.topic
        yield self.partition

    def __len__(self) -> int:
        return 2

    @overload
    def __getitem__(self, index: int) -> str | int: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[str | int, ...]: ...

    def __getitem__(self, index: int | slice) -> str | int | tuple[str | int, ...]:
        return (self.topic, self.partition)[index]

    def __eq__(self, value: object, /) -> bool:
        if isinstance(value, tuple):
            return (self.topic, self.partition) == value
        if isinstance(value, TopicPartition):
            return (self.topic, self.partition) == (value.topic, value.partition)
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))

    def __repr__(self) -> str:
        body = f"{self.topic!r}, {self.partition}"
        if not self.declare:
            body += ", declare=False"
        return f"{self.__class__.__name__}({body})"

    def add_prefix(self, prefix: str) -> "TopicPartition":
        return TopicPartition(
            f"{prefix}{self.topic}",
            self.partition,
            declare=self.declare,
        )
