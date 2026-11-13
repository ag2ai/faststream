class TopicPartition:
    """A Kafka topic partition with its creation setting."""

    __slots__ = (
        "declare",
        "partition",
        "topic",
    )

    def __init__(
        self,
        topic: str,
        partition: int = -1,
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

    def add_prefix(self, prefix: str) -> "TopicPartition":
        return TopicPartition(
            f"{prefix}{self.topic}",
            self.partition,
            declare=self.declare,
        )
