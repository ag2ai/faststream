from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncEngine
from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.configs.broker import SqlaBrokerConfig
from faststream.sqla.subscriber.factory import create_subscriber
from faststream.sqla.retry import RetryStrategy


class SqlaRegistrator(Registrator[Any, Any]):
    def subscriber(
        self,
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
        ack_policy: AckPolicy = AckPolicy.NACK_ON_ERROR,
    ) -> Any:
        workers = max_workers or 1

        subscriber = create_subscriber(
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
            config=cast("SqlaBrokerConfig", self.config),
            ack_policy=ack_policy,
        )

        super().subscriber(subscriber)

        # subscriber.add_call(
        #     parser_=parser,
        #     decoder_=decoder,
        #     dependencies_=dependencies,
        #     middlewares_=middlewares,
        # )

        return subscriber