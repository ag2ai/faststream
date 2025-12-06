from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol


class RetryStrategyProto(Protocol):
    def __init__(self, *args, **kwargs) -> None:
        ...

    def get_next_attempt_at(
        self,
        *,
        first_attempt_at: datetime,
        last_attempt_at: datetime,
        attempts_count: int,
    ) -> datetime | None:
        ...

    def allow_attempt(
        self,
        *,
        first_attempt_at: datetime,
        attempts_count: int,
    ) -> bool:
        ...


@dataclass(kw_only=True)
class RetryStrategyTemplate(ABC):
    max_total_delay_seconds: int | None
    max_attempts: int | None

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


@dataclass(kw_only=True)
class ConstantRetryStrategy(RetryStrategyTemplate):
    delay_seconds: int

    def _get_next_attempt_at(
        self, first_attempt_at: datetime, last_attempt_at: datetime, attempts_count: int
    ) -> datetime | None:
        return last_attempt_at + timedelta(seconds=self.delay_seconds)


@dataclass(kw_only=True)
class NoRetryStrategy(RetryStrategyProto):
    max_attempts = 1

    def get_next_attempt_at(
        self,
        *,
        first_attempt_at: datetime,
        last_attempt_at: datetime,
        attempts_count: int,
    ) -> datetime | None:
        if attempts_count >= self.max_attempts:
            return None
        raise ValueError

    def allow_attempt(
        self,
        *,
        first_attempt_at: datetime,
        attempts_count: int,
    ) -> bool:
        if attempts_count > self.max_attempts:
            return False
        return True