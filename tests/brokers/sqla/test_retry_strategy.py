import random
from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from faststream.sqla.retry import (
    ConstantRetryStrategy,
    ExponentialBackoffWithJitterRetryStrategy,
    ExponentialBackoffRetryStrategy,
    ConstantWithJitterRetryStrategy,
    LinearRetryStrategy,
    NoRetryStrategy,
    RetryStrategyTemplate,
)


@pytest.mark.sqla()
def test_no_retry_strategy() -> None:
    strategy = NoRetryStrategy()
    first_attempt_at = datetime.now(timezone.utc)

    assert (
        strategy.get_next_attempt_at(
            first_attempt_at=first_attempt_at,
            last_attempt_at=first_attempt_at,
            attempts_count=1,
        )
        is None
    )


@pytest.mark.sqla()
def test_constant_retry_strategy() -> None:
    strategy = ConstantRetryStrategy(
        delay_seconds=10, max_total_delay_seconds=60, max_attempts=4
    )
    first_attempt_at = datetime.now(timezone.utc)

    result1 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=first_attempt_at,
        attempts_count=1,
    )
    assert result1 == first_attempt_at + timedelta(seconds=10)

    result2 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result1,
        attempts_count=2,
    )
    assert result2 == result1 + timedelta(seconds=10)

    result3 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result2,
        attempts_count=3,
    )
    assert result3 == result2 + timedelta(seconds=10)

    assert (
        strategy.get_next_attempt_at(
            first_attempt_at=first_attempt_at,
            last_attempt_at=result3,
            attempts_count=4,
        )
        is None
    )


@pytest.mark.sqla()
def test_linear_retry_strategy() -> None:
    strategy = LinearRetryStrategy(
        initial_delay_seconds=1,
        step_seconds=2,
        max_total_delay_seconds=60,
        max_attempts=4,
    )
    first_attempt_at = datetime.now(timezone.utc)

    result1 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=first_attempt_at,
        attempts_count=1,
    )
    assert result1 == first_attempt_at + timedelta(seconds=1)

    result2 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result1,
        attempts_count=2,
    )
    assert result2 == result1 + timedelta(seconds=3)

    result3 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result2,
        attempts_count=3,
    )
    assert result3 == result2 + timedelta(seconds=5)

    assert (
        strategy.get_next_attempt_at(
            first_attempt_at=first_attempt_at,
            last_attempt_at=result3,
            attempts_count=4,
        )
        is None
    )


@pytest.mark.sqla()
def test_exponential_backoff_retry_strategy() -> None:
    strategy = ExponentialBackoffRetryStrategy(
        initial_delay_seconds=1,
        multiplier=2,
        max_delay_seconds=5,
        max_total_delay_seconds=60,
        max_attempts=5,
    )
    first_attempt_at = datetime.now(timezone.utc)

    result1 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=first_attempt_at,
        attempts_count=1,
    )
    assert result1 == first_attempt_at + timedelta(seconds=1)

    result2 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result1,
        attempts_count=2,
    )
    assert result2 == result1 + timedelta(seconds=2)

    result3 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result2,
        attempts_count=3,
    )
    assert result3 == result2 + timedelta(seconds=4)

    result4 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result3,
        attempts_count=4,
    )
    assert result4 == result3 + timedelta(seconds=5)

    assert (
        strategy.get_next_attempt_at(
            first_attempt_at=first_attempt_at,
            last_attempt_at=result4,
            attempts_count=5,
        )
        is None
    )


@pytest.mark.sqla()
def test_constant_with_jitter_retry_strategy() -> None:
    rng = random.Random(42)
    strategy = ConstantWithJitterRetryStrategy(
        base_delay_seconds=10,
        jitter_seconds=2,
        max_total_delay_seconds=60,
        max_attempts=4,
        _random=rng,
    )
    first_attempt_at = datetime.now(timezone.utc)

    result1 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=first_attempt_at,
        attempts_count=1,
    )
    delay1 = (result1 - first_attempt_at).total_seconds()
    assert 8 <= delay1 <= 12

    result2 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result1,
        attempts_count=2,
    )
    delay2 = (result2 - result1).total_seconds()
    assert 8 <= delay2 <= 12

    result3 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result2,
        attempts_count=3,
    )
    delay3 = (result3 - result2).total_seconds()
    assert 8 <= delay3 <= 12

    assert (
        strategy.get_next_attempt_at(
            first_attempt_at=first_attempt_at,
            last_attempt_at=result3,
            attempts_count=4,
        )
        is None
    )


@pytest.mark.sqla()
def test_exponential_backoff_with_jitter_retry_strategy() -> None:
    rng = random.Random(42)
    strategy = ExponentialBackoffWithJitterRetryStrategy(
        initial_delay_seconds=1,
        multiplier=2,
        max_delay_seconds=10,
        jitter_factor=0.5,
        max_total_delay_seconds=60,
        max_attempts=4,
        _random=rng,
    )
    first_attempt_at = datetime.now(timezone.utc)

    result1 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=first_attempt_at,
        attempts_count=1,
    )
    delay1 = (result1 - first_attempt_at).total_seconds()
    assert 1 <= delay1 <= 1.5

    result2 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result1,
        attempts_count=2,
    )
    delay2 = (result2 - result1).total_seconds()
    assert 2 <= delay2 <= 3

    result3 = strategy.get_next_attempt_at(
        first_attempt_at=first_attempt_at,
        last_attempt_at=result2,
        attempts_count=3,
    )
    delay3 = (result3 - result2).total_seconds()
    assert 4 <= delay3 <= 6

    assert (
        strategy.get_next_attempt_at(
            first_attempt_at=first_attempt_at,
            last_attempt_at=result3,
            attempts_count=4,
        )
        is None
    )
