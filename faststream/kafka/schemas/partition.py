from collections.abc import Iterator
from typing import overload


class TopicPartition:
    """A Kafka topic partition with its creation setting.

    The object is class-shaped like Confluent's ``TopicPartition``. Tuple
    operations preserve compatibility with aiokafka's two-field value.
    """

    __slots__ = (
        "declare",
        "partition",
        "topic",
    )

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

    __match_args__ = ("topic", "partition")

    def __setattr__(self, name: str, value: object) -> None:
        if name in {"topic", "partition", "declare"} and hasattr(self, name):
            msg = f"{self.__class__.__name__} is immutable"
            raise AttributeError(msg)
        super().__setattr__(name, value)

    def __iter__(self) -> Iterator[str | int]:
        return iter((self.topic, self.partition))

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
            return self.topic == value.topic and self.partition == value.partition
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
