from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Protocol

from faststream._internal.types import (
    BrokerMiddleware,
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)

from .call_wrapper import (
    HandlerCallWrapper,
    ensure_call_wrapper,
)

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto


class Endpoint(Protocol[MsgType]):
    @property
    def _producer(self) -> "ProducerProto":
        raise NotImplementedError

    @property
    def _broker_middlewares(self) -> Sequence["BrokerMiddleware[MsgType]"]:
        raise NotImplementedError

    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn] | HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn],
    ) -> HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]:
        handler: HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn] = (
            ensure_call_wrapper(func)
        )
        return handler
