from dataclasses import dataclass, field

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.sqs.broker.config import SQSBrokerConfig


@dataclass(kw_only=True)
class SQSPublisherSpecificationConfig(PublisherSpecificationConfig):
    queue: str


@dataclass(kw_only=True)
class SQSPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "SQSBrokerConfig" = field(default_factory=SQSBrokerConfig)

    queue: str
    headers: dict[str, str] | None
    group_id: str | None
    deduplication_id: str | None
    delay_seconds: int
