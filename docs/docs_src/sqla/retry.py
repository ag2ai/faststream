from faststream.sqla.retry import (
    ConstantRetryStrategy,
    ConstantWithJitterRetryStrategy,
    ExponentialBackoffRetryStrategy,
    ExponentialBackoffWithJitterRetryStrategy,
    LinearRetryStrategy,
    NoRetryStrategy,
)

constant = ConstantRetryStrategy(
    delay_seconds=5,
    max_attempts=3,
    max_total_delay_seconds=None,
)

linear = LinearRetryStrategy(
    initial_delay_seconds=1,
    step_seconds=2,
    max_attempts=5,
    max_total_delay_seconds=60,
)

exponential = ExponentialBackoffRetryStrategy(
    initial_delay_seconds=1,
    multiplier=2.0,
    max_delay_seconds=60,
    max_attempts=8,
    max_total_delay_seconds=300,
)

exponential_jitter = ExponentialBackoffWithJitterRetryStrategy(
    initial_delay_seconds=1,
    multiplier=2.0,
    max_delay_seconds=60,
    jitter_factor=0.5,
    max_attempts=8,
    max_total_delay_seconds=300,
)

constant_jitter = ConstantWithJitterRetryStrategy(
    base_delay_seconds=5,
    jitter_seconds=2,
    max_attempts=3,
    max_total_delay_seconds=None,
)

no_retry = NoRetryStrategy()
