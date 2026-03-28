from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Protocol

from faststream.message import AckStatus, StreamMessage


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
    settled: AckStatus | None = None


class MQMessage(StreamMessage[MQRawMessage]):
    async def ack(self) -> None:
        if self.committed is None and self.raw_message.connection is not None:
            try:
                await asyncio.shield(self.raw_message.connection.commit())
            except asyncio.CancelledError:
                pass
        await super().ack()
        self.raw_message.settled = self.committed

    async def nack(self) -> None:
        if self.committed is None and self.raw_message.connection is not None:
            try:
                await asyncio.shield(self.raw_message.connection.backout())
            except asyncio.CancelledError:
                pass
        await super().nack()
        self.raw_message.settled = self.committed

    async def reject(self) -> None:
        await self.ack()
