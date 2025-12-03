from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class RetryStrategy(ABC):
    max_total_delay_seconds: int | None
    max_attempts: int | None

    def __post_init__(self) -> None:
        assert self.max_attempts or self.max_total_delay_seconds

    @abstractmethod
    def _get_next_attempt_at(
        self, first_attempt_at: datetime, last_attempt_at: datetime, attempts_count: int
    ) -> datetime: ...

    def get_next_attempt_at(
        self,
        *,
        first_attempt_at: datetime,
        last_attempt_at: datetime,
        attempts_count: int,
    ) -> datetime | None:
        if self.max_attempts and attempts_count >= self.max_attempts:
            return None
        next_attempt_at = self._get_next_attempt_at(
            first_attempt_at, last_attempt_at, attempts_count
        )
        if self.max_total_delay_seconds and next_attempt_at - first_attempt_at > timedelta(
            seconds=self.max_total_delay_seconds
        ):
            return None
        return next_attempt_at

    def allow_attempt(
        self,
        *,
        first_attempt_at: datetime,
        attempts_count: int,
    ) -> bool:
        if self.max_attempts and attempts_count >= self.max_attempts:
            return False
        if self.max_total_delay_seconds and datetime.now(timezone.utc) - first_attempt_at > timedelta(
            seconds=self.max_total_delay_seconds
        ):
            return False
        return True

@dataclass
class ConstantRetryStrategy(RetryStrategy):
    delay_seconds: int

    def _get_next_attempt_at(
        self, first_attempt_at: datetime, last_attempt_at: datetime, attempts_count: int
    ) -> datetime | None:
        return last_attempt_at + timedelta(seconds=self.delay_seconds)
