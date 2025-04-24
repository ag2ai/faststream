from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from faststream._internal.configs import (
    SpecificationConfigs as SpecificationSubscriberOptions,
    UseCaseConfigs,
)
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        AsyncCallable,
    )

__all__ = ("SpecificationSubscriberOptions",)


@dataclass
class SubscriberUseCaseConfigs(UseCaseConfigs):
    no_reply: bool
    broker_dependencies: Iterable["Dependant"]
    ack_policy: AckPolicy
    default_parser: Optional["AsyncCallable"]
    default_decoder: Optional["AsyncCallable"]
