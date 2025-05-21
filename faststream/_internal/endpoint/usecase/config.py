from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from faststream._internal.types import BrokerMiddleware, MsgType


@dataclass(kw_only=True)
class EndpointConfig:
    broker_middlewares: Sequence["BrokerMiddleware[MsgType]"] = field(default_factory=list)
