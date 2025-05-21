from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.usecase import EndpointConfig
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import AsyncCallable


@dataclass(kw_only=True)
class SubscriberUsecaseConfig(EndpointConfig):
    broker_dependencies: Iterable["Dependant"] = field(default_factory=list)

    default_parser: Optional["AsyncCallable"] = None
    default_decoder: Optional["AsyncCallable"] = None

    no_reply: bool = False

    _ack_policy: AckPolicy = field(default_factory=lambda: EMPTY, repr=False)

    @property
    def ack_policy(self) -> AckPolicy:
        raise NotImplementedError
