from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.confluent.configs import KafkaBrokerConfig
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream.confluent.schemas import TopicPartition


@dataclass(kw_only=True)
class KafkaSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    topics: Sequence[str] = field(default_factory=list)
    partitions: Iterable["TopicPartition"] = field(default_factory=list)


@dataclass(kw_only=True)
class KafkaSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "KafkaBrokerConfig" = field(default_factory=KafkaBrokerConfig)

    topics: Sequence[str] = field(default_factory=list)
    partitions: Sequence["TopicPartition"] = field(default_factory=list)
    polling_interval: float = 0.1
    group_id: str | None = None
    connection_data: dict[str, Any] = field(default_factory=dict)

    _auto_commit: bool = field(default_factory=lambda: EMPTY, repr=False)
    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)

    def __post_init__(self) -> None:
        self.connection_data["enable_auto_commit"] = self.ack_first

    @property
    def ack_first(self) -> bool:
        return self.ack_policy is AckPolicy.ACK_FIRST

    @property
    def auto_ack_disabled(self) -> bool:
        return self.ack_policy in {AckPolicy.MANUAL, AckPolicy.ACK_FIRST}

    @property
    def ack_policy(self) -> AckPolicy:
        if self._auto_commit is not EMPTY:
            return AckPolicy.ACK_FIRST if self._auto_commit else AckPolicy.REJECT_ON_ERROR

        if self._no_ack:
            return AckPolicy.MANUAL

        if self._ack_policy is EMPTY:
            return AckPolicy.ACK_FIRST

        return self._ack_policy
