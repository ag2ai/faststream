from dataclasses import dataclass
from typing import Optional

from .bindings import ChannelBinding
from .operation import Operation


@dataclass
class PublisherSpec:
    description: Optional[str]
    operation: Operation
    bindings: Optional[ChannelBinding]
