from dataclasses import dataclass, field

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.usecase import EndpointConfig
from faststream.middlewares import AckPolicy


@dataclass(kw_only=True)
class SubscriberUsecaseConfig(EndpointConfig):
    no_reply: bool = False

    _ack_policy: AckPolicy = field(default_factory=lambda: EMPTY, repr=False)

    @property
    def ack_policy(self) -> AckPolicy:
        raise NotImplementedError
