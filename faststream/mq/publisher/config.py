from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.mq.configs import MQBrokerConfig, MQConfig


@dataclass(kw_only=True)
class MQPublisherSpecificationConfig(MQConfig, PublisherSpecificationConfig):
    pass


@dataclass(kw_only=True)
class MQPublisherConfig(MQConfig, PublisherUsecaseConfig):
    _outer_config: MQBrokerConfig = field(default_factory=MQBrokerConfig)  # type: ignore[arg-type]

    headers: dict[str, Any] = field(default_factory=dict)
    reply_to: str = ""
    reply_to_qmgr: str = ""
    priority: int | None = None
    persistence: bool | None = None
    expiry: int | None = None
    message_type: str | None = None
