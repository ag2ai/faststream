from dataclasses import dataclass, field
from datetime import datetime, timezone
import enum
from typing import Any, Callable, Coroutine, cast

from faststream.message.message import StreamMessage
from faststream.sqla.retry import RetryStrategyProto


class SqlaMessageState(str, enum.Enum):
    """
    The message starts out as PENDING. When it is acquired by a worker, it is marked as
    PROCESSING. After being acquired, depending on processing result, AckPolicy, retry
    strategy, and presence of manual acknowledgement, the message can be marked as
    COMPLETED, FAILED, or RETRYABLE prior to or after a processing attempt. A message
    that is COMPLETED or FAILED is archived and will not be processed again. A RETRYABLE
    message might be retried.
    """
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYABLE = "retryable"


class SqlaMessage(StreamMessage):
    retry_strategy: RetryStrategyProto

    def __init__(
        self,
        id: int,
        queue: str,
        state: SqlaMessageState,
        payload: bytes,
        attempts_count: int,
        deliveries_count: int,
        created_at: datetime,
        first_attempt_at: datetime,
        next_attempt_at: datetime | None,
        last_attempt_at: datetime | None,
        acquired_at: datetime | None,
    ) -> None:
        self.id = id
        self.queue = queue
        self.state = state
        self.payload = payload
        self.attempts_count = attempts_count
        self.deliveries_count = deliveries_count
        self.created_at = created_at
        self.first_attempt_at = first_attempt_at
        self.next_attempt_at = next_attempt_at
        self.last_attempt_at = last_attempt_at
        self.acquired_at = acquired_at
        
        self.state_set = False
        self.to_archive = False
        
        super().__init__(raw_message=self, body=payload)

    async def ack(self) -> None:
        await self._update_state_if_not_set(self._ack)

    async def nack(self) -> None:
        await self._update_state_if_not_set(self._nack)

    async def reject(self) -> None:
        await self._update_state_if_not_set(self._reject)

    def _record_attempt(self) -> None:
        self.attempts_count += 1
        self.last_attempt_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
        if self.attempts_count == 1:
            self.first_attempt_at = self.last_attempt_at

    def _mark_completed(self) -> None:
        self.state = SqlaMessageState.COMPLETED
        self.to_archive = True

    def _mark_retryable(self, *, next_attempt_at: datetime) -> None:
        self.state = SqlaMessageState.RETRYABLE
        self.next_attempt_at = next_attempt_at

    def _mark_failed(self) -> None:
        self.state = SqlaMessageState.FAILED
        self.to_archive = True

    def _mark_pending(self) -> None:
        self.state = SqlaMessageState.PENDING
        self.deliveries_count -= 1

    def _check_max_deliveries(self, max_deliveries: int | None) -> bool:
        if max_deliveries and self.deliveries_count > max_deliveries:
            self._mark_failed()
            return False
        return True

    async def _update_state_if_not_set(self, update_method: Callable[[], Coroutine[Any, Any, None]]) -> None:
        if self.state_set:
            return
        
        await update_method()
        
        self.state_set = True

    async def _ack(self) -> None:
        self._record_attempt()
        self._mark_completed()

    async def _nack(self) -> None:
        self._record_attempt()
        if not (
            next_attempt_at := self.retry_strategy.get_next_attempt_at(
                first_attempt_at=self.first_attempt_at,
                last_attempt_at=cast(datetime, self.last_attempt_at),
                attempts_count=self.attempts_count,
            )
        ):
            self._mark_failed()
        else:
            self._mark_retryable(next_attempt_at=next_attempt_at)

    async def _reject(self) -> None:
        self._record_attempt()
        self._mark_failed()

    def __repr__(self) -> str: # TODO
        return f"SqlaMessage(id={self.id}, queue={self.queue})"
