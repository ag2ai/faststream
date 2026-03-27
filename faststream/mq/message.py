from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from faststream.message import StreamMessage

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable


class MQAcknowledger(Protocol):
    async def commit(self) -> None: ...

    async def backout(self) -> None: ...


@dataclass
class MQRawMessage:
    body: bytes
    queue: str
    headers: dict[str, Any] = field(default_factory=dict)
    reply_to: str = ""
    reply_to_qmgr: str = ""
    content_type: str | None = None
    correlation_id: str | None = None
    message_id: str | None = None
    native_message_id: bytes | None = None
    native_correlation_id: bytes | None = None
    priority: int | None = None
    persistence: bool | None = None
    expiry: int | None = None
    metadata: Any = None
    connection: MQAcknowledger | None = None


class MQMessage(StreamMessage[MQRawMessage]):
    async def ack(self) -> None:
        if self.committed is None and self.raw_message.connection is not None:
            await self.raw_message.connection.commit()
        await super().ack()

    async def nack(self) -> None:
        if self.committed is None and self.raw_message.connection is not None:
            await self.raw_message.connection.backout()
        await super().nack()

    async def reject(self) -> None:
        await self.ack()
