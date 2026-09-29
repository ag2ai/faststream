from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.constants import EMPTY
from faststream._internal.utils.functions import to_async
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.types import (
        AsyncCallable,
        AsyncExceptionHandler,
        ExceptionHandler,
    )

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
    exception_handler: "ExceptionHandler | None" = None
    _exception_handler: "AsyncExceptionHandler | None" = field(
        default=None,
        init=False,
        repr=False,
    )

    _ack_policy: AckPolicy = field(default_factory=lambda: EMPTY, repr=False)

    parser: "AsyncCallable" = field(init=False)
    decoder: "AsyncCallable" = field(init=False)

    def __post_init__(self) -> None:
        if self.exception_handler is not None:
            self._exception_handler = to_async(self.exception_handler)

    @property
    def auto_ack_disabled(self) -> bool:
        return self.ack_policy is AckPolicy.MANUAL

    @property
    def ack_policy(self) -> AckPolicy:
        raise NotImplementedError
