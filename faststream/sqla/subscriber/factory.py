from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream._internal.endpoint.subscriber.specification import SubscriberSpecification
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.configs.broker import SqlaBrokerConfig
from faststream.sqla.configs.subscriber import SqlaSubscriberConfig
from faststream.sqla.retry import RetryStrategy
from faststream.sqla.subscriber.specification import SqlaSubscriberSpecification
from faststream.sqla.subscriber.usecase import SqlaSubscriber


def create_subscriber(
    engine: AsyncEngine,
    queue: str,
    max_workers: int,
    retry_strategy: RetryStrategy,
    max_fetch_interval: float,
    min_fetch_interval: float,
    fetch_batch_size: int,
    overfetch_factor: float,
    flush_interval: float,
    release_stuck_interval: float,
    graceful_shutdown_timeout: float,
    release_stuck_timeout: int,
    config: "SqlaBrokerConfig",
    ack_policy: AckPolicy,
) -> Any:
    subscriber_config = SqlaSubscriberConfig(
        engine=engine,
        queue=queue,
        max_workers=max_workers,
        retry_strategy=retry_strategy,
        max_fetch_interval=max_fetch_interval,
        min_fetch_interval=min_fetch_interval,
        fetch_batch_size=fetch_batch_size,
        overfetch_factor=overfetch_factor,
        flush_interval=flush_interval,
        release_stuck_interval=release_stuck_interval,
        graceful_shutdown_timeout=graceful_shutdown_timeout,
        release_stuck_timeout=release_stuck_timeout,
        _outer_config=config,
        _ack_policy=ack_policy,
    )

    calls = CallsCollection[Any]()

    specification = SqlaSubscriberSpecification()

    return SqlaSubscriber(subscriber_config, specification, calls)