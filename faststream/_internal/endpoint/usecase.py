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
    def __init__(self, config: "BrokerConfig") -> None:
        self._outer_config = config
        self._prepared = False

    def prepare(self) -> None:
        """Preparation: derive what this endpoint derives, before any I/O.

        Idempotent, so the Broker driving it at `connect()` and an endpoint
        registered afterwards driving its own at `start()` need no coordination.
        """
        if self._prepared:
            return

        self._prepare()
        self._prepared = True

    def _prepare(self) -> None:
        """What this endpoint derives from the options composition.

        Nothing, unless an endpoint has something to derive. Synchronous by
        contract: an implementation that needs the network belongs in `start()`.
        """

    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn],
    ) -> HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]:
        handler: HandlerCallWrapper[P_HandlerParams, T_HandlerReturn] = (
            ensure_call_wrapper(func)
        )
        return handler
