from dataclasses import dataclass

from .bindings import ChannelBinding
from .operation import Operation


@dataclass
class PublisherSpec:
    """One publisher, as the specification generators see it.

    `address` is the Address template the endpoint publishes to, which is not the
    key this spec is filed under — see `SubscriberSpec`.
    """

    description: str | None
    operation: Operation
    bindings: ChannelBinding | None
    address: str
