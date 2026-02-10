from dataclasses import dataclass, field

from faststream._internal.configs.endpoint import SubscriberUsecaseConfig
from faststream._internal.constants import EMPTY
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.configs.broker import SqlaBrokerConfig
from faststream.sqla.retry import RetryStrategyProto


@dataclass(kw_only=True)
class SqlaSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "SqlaBrokerConfig" = field(default_factory=SqlaBrokerConfig)

    queues: list[str]
    max_workers: int
    retry_strategy: RetryStrategyProto | None
    max_fetch_interval: float
    min_fetch_interval: float
    fetch_batch_size: int
    overfetch_factor: float
    flush_interval: float
    release_stuck_interval: float
    release_stuck_timeout: float
    max_deliveries: int | None

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            return AckPolicy.NACK_ON_ERROR
        return self._ack_policy
