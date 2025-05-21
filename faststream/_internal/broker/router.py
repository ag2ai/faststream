from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
)

from faststream._internal.state.broker import EmptyBrokerState
from faststream._internal.types import (
    BrokerMiddleware,
    CustomCallable,
    MsgType,
)

from .abc_broker import ABCBroker

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict


class ArgsContainer:
    """Class to store any arguments."""

    __slots__ = ("args", "kwargs")

    args: Iterable[Any]
    kwargs: "AnyDict"

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.args = args
        self.kwargs = kwargs


class SubscriberRoute(ArgsContainer):
    """A generic class to represent a broker route."""

    __slots__ = ("args", "call", "kwargs", "publishers")

    call: Callable[..., Any]
    publishers: Iterable[Any]

    def __init__(
        self,
        call: Callable[..., Any],
        *args: Any,
        publishers: Iterable[ArgsContainer] = (),
        **kwargs: Any,
    ) -> None:
        """Initialize a callable object with arguments and keyword arguments."""
        self.call = call
        self.publishers = publishers

        super().__init__(*args, **kwargs)


class BrokerRouter(ABCBroker[MsgType]):
    """A generic class representing a broker router."""

    def __init__(
        self,
        *,
        handlers: Iterable[SubscriberRoute],
        # base options
        prefix: str,
        dependencies: Iterable["Dependant"],
        middlewares: Sequence["BrokerMiddleware[MsgType]"],
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        include_in_schema: Optional[bool],
        routers: Sequence["ABCBroker[MsgType]"],
    ) -> None:
        super().__init__(
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            parser=parser,
            decoder=decoder,
            include_in_schema=include_in_schema,
            state=EmptyBrokerState("You should include router to any broker."),
            routers=routers,
        )

        for h in handlers:
            call = h.call

            for p in h.publishers:
                call = self.publisher(*p.args, **p.kwargs)(call)

            self.subscriber(*h.args, **h.kwargs)(call)
