from collections.abc import Callable
from typing import TYPE_CHECKING

from faststream._internal.types import P_HandlerParams, T_HandlerReturn

from .call_wrapper import (
    HandlerCallWrapper,
    ensure_call_wrapper,
)

if TYPE_CHECKING:
    from faststream._internal.configs import BrokerConfig


class Endpoint:
    # Subscribers and publishers keep a `__dict__`: users patch their methods on the
    # instance in tests (`patch.object(publisher, "publish")`), and there are only a
    # few per application. `Registrator` holds them in a WeakSet, hence `__weakref__`.
    __slots__ = ("__dict__", "__weakref__", "_outer_config")

    def __init__(self, config: "BrokerConfig") -> None:
        self._outer_config = config

    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn],
    ) -> HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]:
        handler: HandlerCallWrapper[P_HandlerParams, T_HandlerReturn] = (
            ensure_call_wrapper(func, self._outer_config)
        )
        return handler
