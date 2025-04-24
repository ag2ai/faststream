from collections.abc import Sequence
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Optional,
)

from faststream._internal.subscriber.configs import SubscriberUseCaseConfigs
from faststream.exceptions import SetupError
from faststream.middlewares.acknowledgement.conf import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream.confluent.schemas import TopicPartition


@dataclass
class ConfluentSubscriberBaseConfigs(SubscriberUseCaseConfigs):
    topics: Sequence[str]
    partitions: Sequence["TopicPartition"]
    polling_interval: float
    group_id: Optional[str]
    connection_data: "AnyDict"
    auto_commit: bool
    no_ack: bool

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not self.topics and not self.partitions:
            msg = "You should provide either `topics` or `partitions`."
            raise SetupError(msg)

        if self.topics and self.partitions:
            msg = "You can't provide both `topics` and `partitions`."
            raise SetupError(msg)

        if not self.group_id and self.ack_policy is not AckPolicy.ACK_FIRST:
            msg = "You must use `group_id` with manual commit mode."
            raise SetupError(msg)
