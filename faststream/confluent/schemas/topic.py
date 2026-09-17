from confluent_kafka.admin import NewTopic  # type: ignore[attr-defined]

from faststream._internal.proto import NameRequired


class Topic(NameRequired):
    """A Kafka topic with its creation settings.

    Can be used instead of a plain topic name to configure the topic
    **FastStream** creates for you.

    You can find information about all options in the official Kafka documentation:

    https://kafka.apache.org/documentation/#topicconfigs
    """

    __slots__ = (
        "declare",
        "name",
        "num_partitions",
        "replication_factor",
    )

    def __init__(
        self,
        name: str,
        *,
        num_partitions: int = 1,
        replication_factor: int = 1,
        declare: bool = True,
    ) -> None:
        """Initialize the Kafka topic.

        Args:
            name: Kafka topic name.
            num_partitions: Number of partitions to create the topic with.
            replication_factor: Replication factor to create the topic with.
            declare: Whether to create the topic automatically or just connect to it.
                Missing topics are not created and their absence is not reported,
                so set it to `False` for topics provisioned by someone else.
        """
        super().__init__(name)

        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.declare = declare

    def __repr__(self) -> str:
        if self.declare:
            body = f", num_partitions={self.num_partitions}, replication_factor={self.replication_factor}"
        else:
            body = ", declare=False"

        return f"{self.__class__.__name__}({self.name!r}{body})"

    def __eq__(self, value: object, /) -> bool:
        if not isinstance(value, Topic):
            return NotImplemented

        return (
            self.name == value.name
            and self.num_partitions == value.num_partitions
            and self.replication_factor == value.replication_factor
            and self.declare == value.declare
        )

    def __hash__(self) -> int:
        """Restore hashability, which defining `__eq__` would otherwise remove."""
        return hash(
            (
                self.name,
                self.num_partitions,
                self.replication_factor,
                self.declare,
            ),
        )

    def add_prefix(self, prefix: str) -> "Topic":
        return Topic(
            f"{prefix}{self.name}",
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
            declare=self.declare,
        )

    def to_confluent(self) -> NewTopic:
        return NewTopic(
            self.name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
        )
