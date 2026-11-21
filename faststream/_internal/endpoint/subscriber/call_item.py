from collections import UserList
from collections.abc import Iterable, Reversible, Sequence
from functools import partial
from inspect import unwrap
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    cast,
)

from faststream._internal.types import MsgType, StreamMsgType
from faststream.exceptions import IgnoredException, SetupError
from faststream.specification.asyncapi.utils import to_camelcase

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AsyncFuncAny, Decorator
    from faststream._internal.di import FastDependsConfig
    from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
    from faststream._internal.types import (
        AsyncFilter,
        CustomCallable,
        Decoder,
        Parser,
        SubscriberMiddleware,
    )


class HandlerItem(Generic[MsgType, StreamMsgType]):
    """A class representing handler overloaded item."""

    __slots__ = (
        "decoder",
        "dependant",
        "dependencies",
        "filter",
        "handler",
        "item_decoder",
        "item_parser",
        "parser",
    )

    dependant: Any | None
    # What the handler declared, kept as declared so that composing them again
    # from an unchanged declaration produces an unchanged result.
    item_parser: Optional["CustomCallable"]
    item_decoder: Optional["CustomCallable"]
    # What the last composition made of them.
    parser: Optional["Parser[MsgType, StreamMsgType]"]
    decoder: Optional["Decoder[StreamMsgType]"]

    def __init__(
        self,
        *,
        handler: "HandlerCallWrapper[..., Any]",
        filter: "AsyncFilter[Any]",
        item_parser: Optional["CustomCallable"],
        item_decoder: Optional["CustomCallable"],
        dependencies: Sequence["Dependant"],
    ) -> None:
        self.handler = handler
        self.filter = filter
        self.item_parser = item_parser
        self.item_decoder = item_decoder
        self.parser = None
        self.decoder = None
        self.dependencies = dependencies
        self.dependant = None

    def __repr__(self) -> str:
        filter_call = unwrap(self.filter)
        filter_name = getattr(filter_call, "__name__", str(filter_call))
        return f"<'{self.name}': filter='{filter_name}'>"

    def _setup(
        self,
        *,
        parser: "Parser[MsgType, StreamMsgType]",
        decoder: "Decoder[StreamMsgType]",
        config: "FastDependsConfig",
        broker_dependencies: Sequence["Dependant"],
        _call_decorators: Reversible["Decorator"],
    ) -> None:
        self.parser = parser
        self.decoder = decoder

        self.dependant = self.handler.set_wrapped(
            dependencies=(*broker_dependencies, *self.dependencies),
            _call_decorators=_call_decorators,
            config=config,
        )

    @property
    def name(self) -> str:
        """The name of the original call."""
        if self.handler is None:
            return ""

        caller = unwrap(self.handler._composed_call)
        return getattr(caller, "__name__", str(caller))

    @property
    def description(self) -> str | None:
        """The description of original call."""
        if self.handler is None:
            return None

        caller = unwrap(self.handler._composed_call)
        return getattr(caller, "__doc__", None)

    async def is_suitable(
        self,
        msg: MsgType,
        cache: dict[Any, Any],
    ) -> StreamMsgType | None:
        """Check is message suite for current filter."""
        if not (parser := self.parser) or not (decoder := self.decoder):
            error_msg = "You should setup `HandlerItem` at first."
            raise SetupError(error_msg)

        message = cache[parser] = cast(
            "StreamMsgType",
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
        message: StreamMsgType,
        _extra_middlewares: Iterable["SubscriberMiddleware[Any]"],
    ) -> Any:
        """Execute wrapped handler with consume middlewares."""
        call: AsyncFuncAny = self.handler.call_wrapped

        for middleware in _extra_middlewares:
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


class CallsCollection(UserList[HandlerItem[MsgType, StreamMsgType]]):
    def add_call(self, call: "HandlerItem[MsgType, StreamMsgType]") -> None:
        self.data.append(call)

    @property
    def name(self) -> str | None:
        """The name of the handler call."""
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
