from dataclasses import dataclass, field

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.kafka.configs import KafkaBrokerConfig


@dataclass(kw_only=True)
class KafkaPublisherSpecificationConfig(PublisherSpecificationConfig):
    topic: str


@dataclass(kw_only=True)
class KafkaPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "KafkaBrokerConfig" = field(default_factory=KafkaBrokerConfig)

    key: bytes | str | None
    topic: str
    partition: int | None
    headers: dict[str, str] | None
    reply_to: str
