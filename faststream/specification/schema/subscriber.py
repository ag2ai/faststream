from dataclasses import dataclass

from .bindings import ChannelBinding
from .operation import Operation


@dataclass
class SubscriberSpec:
    description: str | None
    operation: Operation
    bindings: ChannelBinding | None
