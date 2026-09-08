from dataclasses import dataclass

from .bindings import ChannelBinding
from .operation import Operation


@dataclass
class SubscriberSpec:
    """One subscriber, as the specification generators see it.

    `address` is the Address template the endpoint listens on. It is not the key
    this spec is filed under: that key names the channel and carries the handler
    name with it, so the two differ for every subscriber.

    It is `None` where the endpoint has no address — AsyncAPI reads an absent one
    as unknown, which an empty string would not say.
    """

    description: str | None
    operation: Operation
    bindings: ChannelBinding | None
    address: str | None
