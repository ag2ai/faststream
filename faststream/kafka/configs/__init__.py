from dataclasses import dataclass

from .broker import KafkaBrokerConfig
from .specification import (
    KafkaPublisherSpecificationConfig,
    KafkaSubscriberSpecificationConfig,
)
from .usecase import KafkaPublisherConfig, KafkaSubscriberConfig


@dataclass(kw_only=True)
class KafkaSubscriberConfigFacade(
    KafkaSubscriberSpecificationConfig,
    KafkaSubscriberConfig,
):
    pass


@dataclass(kw_only=True)
class KafkaPublisherConfigFacade(
    KafkaPublisherSpecificationConfig,
    KafkaPublisherConfig,
):
    pass


__all__ = (
    "KafkaBrokerConfig",
    "KafkaPublisherConfig",
    "KafkaPublisherConfigFacade",
    "KafkaPublisherSpecificationConfig",
    "KafkaSubscriberConfig",
    "KafkaSubscriberConfigFacade",
    "KafkaSubscriberSpecificationConfig",
)
