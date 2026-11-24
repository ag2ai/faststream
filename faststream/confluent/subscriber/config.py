from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.confluent.configs import KafkaBrokerConfig
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream.confluent.schemas import Topic, TopicPartition


@dataclass(kw_only=True)
class KafkaSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    topics: Sequence["Topic"] = field(default_factory=list)
    partitions: Iterable["TopicPartition"] = field(default_factory=list)


@dataclass(kw_only=True)
class KafkaSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "KafkaBrokerConfig" = field(default_factory=KafkaBrokerConfig)

    topics: Sequence["Topic"] = field(default_factory=list)
    partitions: Sequence["TopicPartition"] = field(default_factory=list)
    polling_interval: float = 0.1
    group_id: str | None = None
    connection_data: dict[str, Any] = field(default_factory=dict)

    @property
    def ack_first(self) -> bool:
        return self.ack_policy is AckPolicy.ACK_FIRST

    @property
    def auto_ack_disabled(self) -> bool:
        return self.ack_policy in {AckPolicy.MANUAL, AckPolicy.ACK_FIRST}

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            if self._outer_config.ack_policy is not EMPTY:
                return self._outer_config.ack_policy
            return AckPolicy.ACK_FIRST

        return self._ack_policy

    def validate_ack_policy(self) -> None:
        """Refuse an ack policy the consumer cannot honour."""
        if self.ack_first:
            return

        if not self.group_id:
            msg = (
                f"ack_policy {self.ack_policy.value!r} commits offsets and needs "
                "a group_id. Pass group_id=, or use ACK_FIRST."
            )
            raise SetupError(msg)

        if self.max_workers > 1:
            msg = (
                f"max_workers={self.max_workers} needs ACK_FIRST, but the ack_policy "
                f"is {self.ack_policy.value!r}. Set max_workers=1, or "
                "ack_policy=AckPolicy.ACK_FIRST on the subscriber."
            )
            raise SetupError(msg)
