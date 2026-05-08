class Topic:
    __slots__ = (
        "name",
        "num_partitions",
        "replication_factor",
    )

    def __init__(
        self,
        name: str,
        num_partitions: int | None = None,
        replication_factor: int | None = None,
    ) -> None:
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor

    def add_prefix(self, prefix: str) -> "Topic":
        return Topic(
            name=f"{prefix}{self.name}",
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Topic):
            return (
                self.name == other.name
                and self.num_partitions == other.num_partitions
                and self.replication_factor == other.replication_factor
            )
        return False

    def __hash__(self) -> int:
        return hash((self.name, self.num_partitions, self.replication_factor))
