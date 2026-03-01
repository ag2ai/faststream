import warnings
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncEngine

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.configs.subscriber import SqlaSubscriberConfig
from faststream.sqla.subscriber.specification import SqlaSubscriberSpecification
from faststream.sqla.subscriber.usecase import SqlaSubscriber

if TYPE_CHECKING:
    from faststream.sqla.configs.broker import SqlaBrokerConfig
    from faststream.sqla.retry import RetryStrategyProto


def create_subscriber(
    queues: list[str],
    max_workers: int,
    retry_strategy: "RetryStrategyProto | None",
    max_fetch_interval: float,
    min_fetch_interval: float,
    fetch_batch_size: int,
    overfetch_factor: float,
    flush_interval: float,
    release_stuck_interval: float,
    release_stuck_timeout: float,
    max_deliveries: int | None,
    config: "SqlaBrokerConfig",
    ack_policy: "AckPolicy",
) -> SqlaSubscriber:
    subscriber_config = SqlaSubscriberConfig(
        queues=queues,
        max_workers=max_workers,
        retry_strategy=retry_strategy,
        max_fetch_interval=max_fetch_interval,
        min_fetch_interval=min_fetch_interval,
        fetch_batch_size=fetch_batch_size,
        overfetch_factor=overfetch_factor,
        flush_interval=flush_interval,
        release_stuck_interval=release_stuck_interval,
        release_stuck_timeout=release_stuck_timeout,
        max_deliveries=max_deliveries,
        _outer_config=config,
        _ack_policy=ack_policy,
    )

    calls = CallsCollection[Any]()

    specification = SqlaSubscriberSpecification()

    return SqlaSubscriber(subscriber_config, specification, calls)  # type: ignore[arg-type]


def _validate_input_for_misconfiguration(
    engine: AsyncEngine,
    queues: list[str],
    max_workers: int,
    retry_strategy: "RetryStrategyProto | None",
    max_fetch_interval: float,
    min_fetch_interval: float,
    fetch_batch_size: int,
    overfetch_factor: float,
    flush_interval: float,
    release_stuck_interval: float,
    release_stuck_timeout: float,
    graceful_shutdown_timeout: float,
    max_deliveries: int | None,
    config: "SqlaBrokerConfig",
    ack_policy: AckPolicy,
) -> None:
    if max_deliveries and max_deliveries <= 0:
        msg = "max_deliveries must be a positive integer or None."
        raise SetupError(msg)

    if max_deliveries:
        warnings.warn(
            "Be aware the setting max_deliveries violates the at most once processing guarantee.",
            UserWarning,
            stacklevel=4,
        )

    if ack_policy is AckPolicy.REJECT_ON_ERROR and retry_strategy is not None:
        warnings.warn(
            "Be aware that retry_strategy is ignored when AckPolicy.REJECT_ON_ERROR is used.",
            UserWarning,
            stacklevel=4,
        )

    if retry_strategy is None and ack_policy is AckPolicy.NACK_ON_ERROR:
        warnings.warn(
            "Be aware that if retry_strategy is None, AckPolicy.NACK_ON_ERROR has the same "
            "effect as AckPolicy.REJECT_ON_ERROR.",
            UserWarning,
            stacklevel=4,
        )

    if ack_policy is AckPolicy.ACK_FIRST:
        warnings.warn(
            "Be aware that AckPolicy.ACK_FIRST is the same as AckPolicy.ACK for this broker.",
            UserWarning,
            stacklevel=4,
        )
