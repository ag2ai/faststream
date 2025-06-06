from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Optional,
    Union,
)

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.publisher import PublisherUsecaseConfig
from faststream._internal.endpoint.subscriber import (
    SubscriberUsecaseConfig,
)
from faststream.middlewares.acknowledgement.conf import AckPolicy

from .broker import KafkaBrokerConfig

if TYPE_CHECKING:
    from aiokafka import TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener

    from faststream._internal.basic_types import AnyDict


@dataclass(kw_only=True)
class KafkaPublisherConfig(PublisherUsecaseConfig):
    config: KafkaBrokerConfig = field(default_factory=KafkaBrokerConfig)

    key: Union[bytes, str, None]
    topic: str
    partition: Optional[int]
    headers: Optional[dict[str, str]]
    reply_to: Optional[str]


@dataclass(kw_only=True)
class KafkaSubscriberConfig(SubscriberUsecaseConfig):
    config: KafkaBrokerConfig = field(default_factory=KafkaBrokerConfig)

    topics: Sequence[str] = field(default_factory=list)
    group_id: Optional[str] = None
    connection_args: "AnyDict" = field(default_factory=dict)
    listener: Optional["ConsumerRebalanceListener"] = None
    pattern: Optional[str] = None
    partitions: Iterable["TopicPartition"] = field(default_factory=list)

    _auto_commit: bool = field(default_factory=lambda: EMPTY, repr=False)
    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)

    def __post_init__(self) -> None:
        if self.ack_first:
            self.connection_args["enable_auto_commit"] = True

    @property
    def ack_first(self) -> bool:
        return self.__ack_policy is AckPolicy.ACK_FIRST

    @property
    def ack_policy(self) -> AckPolicy:
        if (policy := self.__ack_policy) is AckPolicy.ACK_FIRST:
            return AckPolicy.DO_NOTHING

        return policy

    @property
    def __ack_policy(self) -> AckPolicy:
        if self._auto_commit is not EMPTY and self._auto_commit:
            return AckPolicy.ACK_FIRST

        if self._no_ack is not EMPTY and self._no_ack:
            return AckPolicy.DO_NOTHING

        if self._ack_policy is EMPTY:
            return AckPolicy.ACK_FIRST

        return self._ack_policy
