from collections import UserList
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

from faststream._internal.types import MsgType
from faststream.exceptions import IgnoredException, SetupError
from faststream.specification.asyncapi.utils import to_camelcase

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AsyncFuncAny, Decorator
    from faststream._internal.di import FastDependsConfig
    from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
    from faststream._internal.types import (
        AsyncCallable,
        AsyncFilter,
        CustomCallable,
        SubscriberMiddleware,
    )
    from faststream.message import StreamMessage


class HandlerItem(Generic[MsgType]):
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

    dependant: Any | None

    def __init__(
        self,
        *,
        handler: "HandlerCallWrapper[..., Any]",
        filter: "AsyncFilter[Any]",
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
        return f"<'{self.name}': filter='{filter_name}'>"

    def _setup(
        self,
        *,
        parser: "AsyncCallable",
        decoder: "AsyncCallable",
        config: "FastDependsConfig",
        broker_dependencies: Iterable["Dependant"],
        _call_decorators: Reversible["Decorator"],
    ) -> None:
        if self.dependant is None:
            self.item_parser = parser
            self.item_decoder = decoder

        self.dependant = self.handler.set_wrapped(
            dependencies=(*broker_dependencies, *self.dependencies),
            _call_decorators=_call_decorators,
            config=config,
        )

    @property
    def name(self) -> str:
        """Returns the name of the original call."""
        if self.handler is None:
            return ""

        caller = unwrap(self.handler._original_call)
        return getattr(caller, "__name__", str(caller))

    @property
    def description(self) -> str | None:
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
        if not (parser := cast("AsyncCallable | None", self.item_parser)) or not (
            decoder := cast("AsyncCallable | None", self.item_decoder)
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


class CallsCollection(UserList[HandlerItem[MsgType]]):
    def add_call(self, call: "HandlerItem[MsgType]") -> None:
        self.data.append(call)

    @property
    def name(self) -> str | None:
        """Returns the name of the handler call."""
        if not self.data:
            return None

        if len(self.data) == 1:
            return to_camelcase(self.data[0].name)

        return f"[{','.join(to_camelcase(c.name) for c in self.data)}]"

    @property
    def description(self) -> str | None:
        if not self.data:
            return None

        if len(self.data) == 1:
            return self.data[0].description

        return "\n".join(
            f"{to_camelcase(h.name)}: {h.description or ''}" for h in self.data
        )
