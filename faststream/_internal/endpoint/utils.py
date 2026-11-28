import inspect
from collections.abc import Awaitable, Callable, Iterable
from contextlib import AsyncExitStack
from functools import partial
from typing import TYPE_CHECKING, Generic, Optional, TypeVar, cast

from faststream._internal.types import MsgType
from faststream._internal.utils.functions import return_input, to_async
from faststream.message.source_type import SourceType

if TYPE_CHECKING:
    from faststream._internal.types import CustomCallable, Decoder, Parser, StreamMsgType
    from faststream.middlewares import BaseMiddleware


async def process_msg(
    msg: MsgType | None,
    *,
    middlewares: Iterable["BaseMiddleware"],
    parser: "Parser[MsgType, StreamMsgType]",
    decoder: "Decoder[StreamMsgType]",
    source_type: SourceType = SourceType.CONSUME,
) -> "StreamMsgType | None":
    if msg is None:
        return None

    async with AsyncExitStack() as stack:
        return_msg: Callable[
            [StreamMsgType],
            Awaitable[StreamMsgType],
        ] = return_input

        for m in middlewares:
            await stack.enter_async_context(m)
            return_msg = partial(m.consume_scope, return_msg)

        parsed_msg = await parser(msg)
        parsed_msg.source_type = source_type
        parsed_msg.set_decoder(decoder)
        return await return_msg(parsed_msg)

    error_msg = "unreachable"
    raise AssertionError(error_msg)


_In = TypeVar("_In")
_Out = TypeVar("_Out")


class ParserComposition(Generic[_In, _Out]):
    __slots__ = (
        "custom_func",
        "default_func",
        "wrapped_func",
    )

    def __init__(
        self,
        custom_func: Optional["CustomCallable"],
        default_func: Callable[[_In], Awaitable[_Out]],
    ) -> None:
        self.custom_func = custom_func
        self.default_func = default_func

        if custom_func is None:
            self.wrapped_func = default_func
        else:
            original_params = inspect.signature(custom_func).parameters

            if len(original_params) == 1:
                custom_func = cast("Callable[[_In], Awaitable[_Out]]", custom_func)
                self.wrapped_func = to_async(custom_func)

            else:
                name = tuple(original_params.items())[1][0]
                self.wrapped_func = cast(
                    "Callable[[_In], Awaitable[_Out]]",
                    partial(to_async(custom_func), **{name: default_func}),
                )

    def __call__(self, msg: _In) -> Awaitable[_Out]:
        return self.wrapped_func(msg)
