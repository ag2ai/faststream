from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.config_value import Configurable
from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.rabbit.configs import RabbitBrokerConfig, RabbitConfig

if TYPE_CHECKING:
    from .options import PublishKwargs


@dataclass(kw_only=True)
class RabbitPublisherSpecificationConfig(
    RabbitConfig,
    PublisherSpecificationConfig,
):
    routing_key: Configurable[str]
    # Held apart from `message_kwargs` rather than inside it: that TypedDict is
    # also the signature of a runtime `publish()` call, which takes no
    # placeholder (ADR-0002).
    reply_to: Configurable[str] | None
    message_kwargs: "PublishKwargs"


@dataclass(kw_only=True)
class RabbitPublisherConfig(RabbitConfig, PublisherUsecaseConfig):
    _outer_config: "RabbitBrokerConfig" = field(default_factory=RabbitBrokerConfig)

    routing_key: Configurable[str]
    reply_to: Configurable[str] | None
    message_kwargs: "PublishKwargs"
