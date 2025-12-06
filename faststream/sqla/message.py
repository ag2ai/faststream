from dataclasses import dataclass, field
from datetime import datetime, timezone
import enum

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
        created_at: datetime,
        first_attempt_at: datetime | None,
        next_attempt_at: datetime | None,
        last_attempt_at: datetime | None,
        acquired_at: datetime | None,
    ) -> None:
        self.id = id
        self.queue = queue
        self.state = state
        self.payload = payload
        self.attempts_count = attempts_count
        self.created_at = created_at
        self.first_attempt_at = first_attempt_at
        self.next_attempt_at = next_attempt_at
        self.last_attempt_at = last_attempt_at
        self.acquired_at = acquired_at
        
        self.decision_recorded = False
        self.to_archive = False
        
        super().__init__(raw_message=self, body=payload)

    def _mark_completed(self) -> None:
        self.state = SqlaMessageState.COMPLETED
        self.next_attempt_at = None
        self.to_archive = True

    def _mark_retryable(self, *, next_attempt_at: datetime) -> None:
        self.state = SqlaMessageState.RETRYABLE
        self.next_attempt_at = next_attempt_at

    def _mark_failed(self) -> None:
        self.state = SqlaMessageState.FAILED
        self.next_attempt_at = None
        self.to_archive = True

    def _mark_pending(self) -> None:
        self.state = SqlaMessageState.PENDING
        self.acquired_at = None

    def _allow_attempt(self) -> bool:
        self.last_attempt_at = datetime.now(timezone.utc)
        if self.attempts_count == 1:
            self.first_attempt_at = self.last_attempt_at
        if not self.retry_strategy.allow_attempt(
            first_attempt_at=self.first_attempt_at,
            attempts_count=self.attempts_count,
        ):
            self._mark_failed()
            return False
        return True

    async def ack(self) -> None:
        if self.decision_recorded:
            return

        self._mark_completed()
        self.decision_recorded = True

    async def nack(self) -> None:
        if self.decision_recorded:
            return

        if not (
            next_attempt_at := self.retry_strategy.get_next_attempt_at(
                first_attempt_at=self.first_attempt_at,
                last_attempt_at=self.last_attempt_at,
                attempts_count=self.attempts_count,
            )
        ):
            self._mark_failed()
        else:
            self._mark_retryable(next_attempt_at=next_attempt_at)
        self.decision_recorded = True

    async def reject(self) -> None:
        if self.decision_recorded:
            return

        self._mark_failed()
        self.decision_recorded = True
    
    def __repr__(self) -> str: # TODO
        return f"SqlaMessage(id={self.id}, queue={self.queue})"
