from typing import TYPE_CHECKING

from confluent_kafka import TopicPartition as ConfluentPartition

if TYPE_CHECKING:
    from typing_extensions import NotRequired, TypedDict

    class _TopicKwargs(TypedDict):
        topic: str
        partition: int
        offset: int
        metadata: NotRequired[str]
        leader_epoch: NotRequired[int]


class TopicPartition:
    __slots__ = (
        "leader_epoch",
        "metadata",
        "offset",
        "partition",
        "topic",
    )

    def __init__(
        self,
        topic: str,
        partition: int = -1,
        offset: int = -1001,
        metadata: str | None = None,
        leader_epoch: int | None = None,
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.metadata = metadata
        self.leader_epoch = leader_epoch

    def to_confluent(self) -> ConfluentPartition:
        kwargs: _TopicKwargs = {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
        }
        if self.metadata is not None:
            kwargs["metadata"] = self.metadata
        if self.leader_epoch is not None:
            kwargs["leader_epoch"] = self.leader_epoch
        return ConfluentPartition(**kwargs)

    def add_prefix(self, prefix: str) -> "TopicPartition":
        return TopicPartition(
            topic=f"{prefix}{self.topic}",
            partition=self.partition,
            offset=self.offset,
            metadata=self.metadata,
            leader_epoch=self.leader_epoch,
        )
