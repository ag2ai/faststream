from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.kafka.configs import KafkaBrokerConfig
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from aiokafka.abc import ConsumerRebalanceListener

    from faststream.kafka.schemas import Topic, TopicPartition


@dataclass(kw_only=True)
class KafkaSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    topics: Sequence["Topic"] = field(default_factory=list)
    partitions: Iterable["TopicPartition"] = field(default_factory=list)
    pattern: str | None = None


@dataclass(kw_only=True)
class KafkaSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "KafkaBrokerConfig" = field(default_factory=KafkaBrokerConfig)

    topics: Sequence["Topic"] = field(default_factory=list)
    group_id: str | None = None
    connection_args: dict[str, Any] = field(default_factory=dict)
    listener: Optional["ConsumerRebalanceListener"] = None
    pattern: str | None = None
    partitions: Iterable["TopicPartition"] = field(default_factory=list)

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

        if self.group_id is None:
            msg = (
                f"ack_policy {self.ack_policy.value!r} commits offsets and needs "
                "a group_id. Pass group_id=, or use ACK_FIRST."
            )
            raise SetupError(msg)

        # Only a subscriber declared before its router was included gets here:
        # the partition-aware mode is picked from the policy visible at declaration.
        if self.max_workers > 1:
            msg = (
                f"max_workers={self.max_workers} needs ACK_FIRST, but ack_policy "
                f"{self.ack_policy.value!r} is inherited from the broker or router. "
                "Set max_workers=1, or declare ack_policy= on the subscriber."
            )
            raise SetupError(msg)
