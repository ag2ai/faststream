from typing import Callable, Protocol, Union

from faststream._internal.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)

from .call_wrapper import (
    HandlerCallWrapper,
    ensure_call_wrapper,
)


class EndpointWrapper(Protocol[MsgType]):
    def __call__(
        self,
        func: Union[
            Callable[P_HandlerParams, T_HandlerReturn],
            HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn],
        ],
    ) -> HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]:
        handler: HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn] = (
            ensure_call_wrapper(func)
        )
        return handler
