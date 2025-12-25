import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
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


@dataclass(kw_only=True)
class RetryStrategyTemplate(ABC, RetryStrategyProto):
    max_total_delay_seconds: float | None
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


@dataclass(kw_only=True)
class ConstantRetryStrategy(RetryStrategyTemplate):
    delay_seconds: float

    def _get_next_attempt_at(
        self, first_attempt_at: datetime, last_attempt_at: datetime, attempts_count: int
    ) -> datetime:
        return last_attempt_at + timedelta(seconds=self.delay_seconds)


@dataclass(kw_only=True)
class LinearRetryStrategy(RetryStrategyTemplate):
    initial_delay_seconds: float
    step_seconds: float

    def _get_next_attempt_at(
        self, first_attempt_at: datetime, last_attempt_at: datetime, attempts_count: int
    ) -> datetime:
        delay = self.initial_delay_seconds + self.step_seconds * (attempts_count - 1)
        return last_attempt_at + timedelta(seconds=delay)


@dataclass(kw_only=True)
class ExponentialBackoffRetryStrategy(RetryStrategyTemplate):
    initial_delay_seconds: float
    multiplier: float = 2.0
    max_delay_seconds: float | None = None

    def _get_next_attempt_at(
        self, first_attempt_at: datetime, last_attempt_at: datetime, attempts_count: int
    ) -> datetime:
        delay = self.initial_delay_seconds * (self.multiplier ** (attempts_count - 1))
        if self.max_delay_seconds:
            delay = min(delay, self.max_delay_seconds)
        return last_attempt_at + timedelta(seconds=delay)


@dataclass(kw_only=True)
class ConstantWithJitterRetryStrategy(RetryStrategyTemplate):
    base_delay_seconds: float
    jitter_seconds: float
    _random: random.Random = field(default_factory=random.Random)

    def _get_next_attempt_at(
        self, first_attempt_at: datetime, last_attempt_at: datetime, attempts_count: int
    ) -> datetime:
        jitter = self._random.uniform(-self.jitter_seconds, self.jitter_seconds)
        delay = max(0, self.base_delay_seconds + jitter)
        return last_attempt_at + timedelta(seconds=delay)


@dataclass(kw_only=True)
class ExponentialBackoffWithJitterRetryStrategy(RetryStrategyTemplate):
    initial_delay_seconds: float
    multiplier: float = 2.0
    max_delay_seconds: float | None = None
    jitter_factor: float = 0.5
    _random: random.Random = field(default_factory=random.Random)

    def _get_next_attempt_at(
        self, first_attempt_at: datetime, last_attempt_at: datetime, attempts_count: int
    ) -> datetime:
        delay = self.initial_delay_seconds * (self.multiplier ** (attempts_count - 1))
        if self.max_delay_seconds is not None:
            delay = min(delay, self.max_delay_seconds)
        jitter = self._random.uniform(0, delay * self.jitter_factor)
        delay = delay + jitter
        return last_attempt_at + timedelta(seconds=delay)


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