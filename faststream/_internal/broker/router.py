from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
)

from faststream._internal.types import MsgType

from .abc_broker import Registrator

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream._internal.configs import BrokerConfig


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

    __slots__ = ("call", "publishers")

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


class BrokerRouter(Registrator[MsgType]):
    """A generic class representing a broker router."""

    def __init__(
        self,
        *,
        config: "BrokerConfig",
        handlers: Iterable[SubscriberRoute],
        routers: Sequence["Registrator[MsgType]"],
    ) -> None:
        super().__init__(
            config=config,
            routers=routers,
        )

        for h in handlers:
            call = h.call

            for p in h.publishers:
                call = self.publisher(*p.args, **p.kwargs)(call)

            self.subscriber(*h.args, **h.kwargs)(call)
