from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqs.configs import SQSBrokerConfig

if TYPE_CHECKING:
    from faststream.sqs.schemas import SQSQueue


@dataclass(kw_only=True)
class SQSSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    queue: str


@dataclass(kw_only=True)
class SQSSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "SQSBrokerConfig" = field(default_factory=SQSBrokerConfig)

    queue: str
    declare: "SQSQueue"
    wait_time_seconds: int = 20
    max_messages: int = 10
    visibility_timeout: int | None = None
    batch: bool = False
    # ReceiveRequestAttemptId — FIFO-only receive-retry dedup token.
    request_attempt_id: str | None = None
    # Opt-in ChangeMessageVisibility heartbeat for long-running handlers.
    extend_visibility: bool = False

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            if self._outer_config.ack_policy is not EMPTY:
                return self._outer_config.ack_policy
            return AckPolicy.ACK
        return self._ack_policy
