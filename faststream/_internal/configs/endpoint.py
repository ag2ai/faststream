from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.constants import EMPTY
from faststream._internal.utils import apply_types
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable, AsyncExceptionHandler

    from .broker import BrokerConfig


@dataclass(kw_only=True)
class EndpointConfig:
    _outer_config: "BrokerConfig"


@dataclass(kw_only=True)
class PublisherUsecaseConfig(EndpointConfig):
    pass


@dataclass(kw_only=True)
class SubscriberUsecaseConfig(EndpointConfig):
    no_reply: bool = False
    exception_handler: "AsyncExceptionHandler | None" = None

    _ack_policy: AckPolicy = field(default_factory=lambda: EMPTY, repr=False)

    parser: "AsyncCallable" = field(init=False)
    decoder: "AsyncCallable" = field(init=False)

    def __post_init__(self) -> None:
        if self.exception_handler is not None:
            self.exception_handler = apply_types(
                self.exception_handler,
                serializer_cls=self._outer_config.fd_config._serializer,
                context__=self._outer_config.context,
            )

    @property
    def auto_ack_disabled(self) -> bool:
        return self.ack_policy is AckPolicy.MANUAL

    @property
    def ack_policy(self) -> AckPolicy:
        raise NotImplementedError
