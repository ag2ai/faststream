import inspect
from collections.abc import Awaitable, Callable, Iterable
from contextlib import AsyncExitStack
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, cast

from faststream._internal.types import MsgType
from faststream._internal.utils.functions import return_input, to_async
from faststream.message.source_type import SourceType

if TYPE_CHECKING:
    from faststream._internal.types import (
        AsyncCallable,
        CustomCallable,
        SyncCallable,
    )
    from faststream.message import StreamMessage
    from faststream.middlewares import BaseMiddleware


async def process_msg(
    msg: MsgType | None,
    *,
    middlewares: Iterable["BaseMiddleware"],
    parser: Callable[[MsgType], Awaitable["StreamMessage[MsgType]"]],
    decoder: Callable[["StreamMessage[MsgType]"], "Any"],
    source_type: SourceType = SourceType.CONSUME,
) -> Optional["StreamMessage[MsgType]"]:
    if msg is None:
        return None

    async with AsyncExitStack() as stack:
        return_msg: Callable[
            [StreamMessage[MsgType]],
            Awaitable[StreamMessage[MsgType]],
        ] = return_input

        for m in middlewares:
            await stack.enter_async_context(m)
            return_msg = partial(m.consume_scope, return_msg)

        parsed_msg = await parser(msg)
        parsed_msg._source_type = source_type
        parsed_msg.set_decoder(decoder)
        return await return_msg(parsed_msg)

    error_msg = "unreachable"
    raise AssertionError(error_msg)


def resolve_custom_func(
    custom_func: Optional["CustomCallable"],
    default_func: "AsyncCallable",
) -> "AsyncCallable":
    """Resolve a custom parser/decoder with default one."""
    if custom_func is None:
        return default_func

    original_params = inspect.signature(custom_func).parameters

    if len(original_params) == 1:
        return to_async(cast("SyncCallable | AsyncCallable", custom_func))

    name = tuple(original_params.items())[1][0]
    return partial(to_async(custom_func), **{name: default_func})
