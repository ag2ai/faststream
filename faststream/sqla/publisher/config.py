from dataclasses import dataclass, field

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.sqla.configs.broker import SqlaBrokerConfig


@dataclass(kw_only=True)
class SqlaPublisherSpecificationConfig(PublisherSpecificationConfig):
    queue: str


@dataclass(kw_only=True)
class SqlaPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "SqlaBrokerConfig" = field(default_factory=SqlaBrokerConfig)
