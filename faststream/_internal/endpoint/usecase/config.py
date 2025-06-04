from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerConfig


@dataclass(kw_only=True)
class EndpointConfig:
    config: "BrokerConfig"
