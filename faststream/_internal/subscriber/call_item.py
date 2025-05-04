from collections.abc import Iterable, Reversible, Sequence
from functools import partial
from inspect import unwrap
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    cast,
)

from typing_extensions import override

from faststream._internal.state import SetupAble
from faststream._internal.types import MsgType
from faststream.exceptions import IgnoredException, SetupError

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AsyncFuncAny, Decorator
    from faststream._internal.state import BrokerState, Pointer
    from faststream._internal.subscriber.call_wrapper import HandlerCallWrapper
    from faststream._internal.types import (
        AsyncCallable,
        AsyncFilter,
        CustomCallable,
        SubscriberMiddleware,
    )
    from faststream.message import StreamMessage


class HandlerItem(SetupAble, Generic[MsgType]):
    """A class representing handler overloaded item."""

    __slots__ = (
        "dependant",
        "dependencies",
        "filter",
        "handler",
        "item_decoder",
        "item_middlewares",
        "item_parser",
    )

    dependant: Optional[Any]

    def __init__(
        self,
        *,
        handler: "HandlerCallWrapper[MsgType, ..., Any]",
        filter: "AsyncFilter[StreamMessage[MsgType]]",
        item_parser: Optional["CustomCallable"],
        item_decoder: Optional["CustomCallable"],
        item_middlewares: Sequence["SubscriberMiddleware[StreamMessage[MsgType]]"],
        dependencies: Iterable["Dependant"],
    ) -> None:
        self.handler = handler
        self.filter = filter
        self.item_parser = item_parser
        self.item_decoder = item_decoder
        self.item_middlewares = item_middlewares
        self.dependencies = dependencies
        self.dependant = None

    def __repr__(self) -> str:
        filter_call = unwrap(self.filter)
        filter_name = getattr(filter_call, "__name__", str(filter_call))
        return f"<'{self.call_name}': filter='{filter_name}'>"

    @override
    def _setup(  # type: ignore[override]
        self,
        *,
        parser: "AsyncCallable",
        decoder: "AsyncCallable",
        state: "Pointer[BrokerState]",
        broker_dependencies: Iterable["Dependant"],
        _call_decorators: Reversible["Decorator"],
    ) -> None:
        if self.dependant is None:
            di_state = state.get().di_state

            self.item_parser = parser
            self.item_decoder = decoder

            dependencies = (*broker_dependencies, *self.dependencies)

            dependant = self.handler.set_wrapped(
                dependencies=dependencies,
                _call_decorators=(*_call_decorators, *di_state.call_decorators),
                state=di_state,
            )

            if di_state.get_dependent is None:
                self.dependant = dependant
            else:
                self.dependant = di_state.get_dependent(
                    self.handler._original_call,
                    dependencies,
                )

    @property
    def call_name(self) -> str:
        """Returns the name of the original call."""
        if self.handler is None:
            return ""

        caller = unwrap(self.handler._original_call)
        return getattr(caller, "__name__", str(caller))

    @property
    def description(self) -> Optional[str]:
        """Returns the description of original call."""
        if self.handler is None:
            return None

        caller = unwrap(self.handler._original_call)
        return getattr(caller, "__doc__", None)

    async def is_suitable(
        self,
        msg: MsgType,
        cache: dict[Any, Any],
    ) -> Optional["StreamMessage[MsgType]"]:
        """Check is message suite for current filter."""
        if not (parser := cast("Optional[AsyncCallable]", self.item_parser)) or not (
            decoder := cast("Optional[AsyncCallable]", self.item_decoder)
        ):
            error_msg = "You should setup `HandlerItem` at first."
            raise SetupError(error_msg)

        message = cache[parser] = cast(
            "StreamMessage[MsgType]",
            cache.get(parser) or await parser(msg),
        )

        # NOTE: final decoder will be set for success filter
        message.set_decoder(decoder)

        if await self.filter(message):
            return message

        return None

    async def call(
        self,
        /,
        message: "StreamMessage[MsgType]",
        _extra_middlewares: Iterable["SubscriberMiddleware[Any]"],
    ) -> Any:
        """Execute wrapped handler with consume middlewares."""
        call: AsyncFuncAny = self.handler.call_wrapped

        for middleware in chain(self.item_middlewares[::-1], _extra_middlewares):
            call = partial(middleware, call)

        try:
            result = await call(message)

        except (IgnoredException, SystemExit):
            self.handler.trigger()
            raise

        except Exception as e:
            self.handler.trigger(error=e)
            raise

        else:
            self.handler.trigger(result=result)
            return result
