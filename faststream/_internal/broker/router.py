from collections.abc import Callable, Iterable
from typing import Any

from faststream._internal.configs import BrokerConfigType
from faststream._internal.types import MsgType

from .registrator import Registrator


class ArgsContainer:
    """Class to store any arguments."""

    __slots__ = ("args", "kwargs")

    args: Iterable[Any]
    kwargs: "dict[str, Any]"

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


class BrokerRouter(Registrator[MsgType, BrokerConfigType]):
    """A generic class representing a broker router."""

    def __init__(
        self,
        *,
        config: BrokerConfigType,
        handlers: Iterable[SubscriberRoute],
        routers: Iterable["Registrator[Any, Any]"],
    ) -> None:
        super().__init__(
            config=config,
            routers=routers,
        )

        # protect handlers from gc
        self._handlers = []

        for h in handlers:
            call = h.call

            for p in h.publishers:
                call = self.publisher(*p.args, **p.kwargs)(call)

            call = self.subscriber(*h.args, **h.kwargs)(call)

            self._handlers.append(call)
