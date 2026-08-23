from collections.abc import Callable
from typing import TYPE_CHECKING

from faststream._internal.endpoint.derived import DerivedReads
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
        self._derived = DerivedReads()

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

    def invalidate(self) -> None:
        """Undo Preparation, so the next `connect()` performs it again.

        ADR-0004 fixes a Config value at `connect()`. Everything derived from
        the composition is kept rather than re-derived on every read, which
        without this would fix the value at the *first* `connect()` instead —
        a second `TestBroker` context over one Broker would silently reuse the
        first context's addresses.

        Driven wherever the connection is cleared, and unconditional rather
        than guarded by `_prepared`: a read taken outside any connection — an
        AsyncAPI render, a `repr` — fills the same memos without preparing
        anything.
        """
        self._prepared = False
        self._derived.reset()
        self._invalidate()

    def _invalidate(self) -> None:
        """Whatever this endpoint keeps that is not a registered read.

        Nothing, unless an endpoint keeps something. A memoised read belongs in
        `self._derived` at construction instead, which needs no override here.
        """

    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn],
    ) -> HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]:
        handler: HandlerCallWrapper[P_HandlerParams, T_HandlerReturn] = (
            ensure_call_wrapper(func)
        )
        return handler
