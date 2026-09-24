from collections.abc import Awaitable, Callable
from typing import Any

from typing_extensions import assert_type

from faststream import BaseMiddleware, ContextRepo
from faststream.kafka import KafkaPublishCommand


class RawMessage:
    topic: str


class Plain(BaseMiddleware):
    async def on_receive(self) -> None:
        assert_type(self.msg, Any | None)

    async def publish_scope(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        cmd: Any,
    ) -> Any:
        return await call_next(cmd)


class Typed(BaseMiddleware[KafkaPublishCommand, RawMessage]):
    async def on_receive(self) -> None:
        assert_type(self.msg, RawMessage)
        assert_type(self.msg.topic, str)

    async def publish_scope(
        self,
        call_next: Callable[[KafkaPublishCommand], Awaitable[Any]],
        cmd: KafkaPublishCommand,
    ) -> Any:
        assert_type(cmd, KafkaPublishCommand)
        return await call_next(cmd)


def check_plain_init(msg: RawMessage, context: ContextRepo) -> None:
    assert_type(Plain(None, context=context).msg, Any | None)
    assert_type(Plain(msg, context=context).msg, Any | None)


def check_typed_init(msg: RawMessage, context: ContextRepo) -> None:
    assert_type(Typed(msg, context=context).msg, RawMessage)
    Typed(None, context=context)  # type: ignore[arg-type]
