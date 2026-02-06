from typing import Any

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.broker.broker import SqlaBroker
from faststream.sqla.broker.router import SqlaRouter
from faststream.sqla.retry import NoRetryStrategy
from tests.brokers.base.basic import BaseTestcaseConfig


class SqlaTestcaseConfig(BaseTestcaseConfig):
    _engine: AsyncEngine | None = None

    @pytest_asyncio.fixture(autouse=True)
    async def setup_engine(self, engine: AsyncEngine, recreate_tables: None) -> None:
        self._engine = engine

    def get_broker(
        self,
        **kwargs: Any,
    ) -> SqlaBroker:
        engine = kwargs.pop("engine", None) or self._engine
        return SqlaBroker(engine=engine, **kwargs)

    def patch_broker(self, broker: SqlaBroker, **kwargs: Any) -> SqlaBroker:
        return broker

    def get_router(self, **kwargs: Any) -> SqlaRouter:
        return SqlaRouter(**kwargs)

    def get_subscriber_params(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if args:
            kwargs.setdefault("queues", [args[0]])
            args = args[1:]

        kwargs.setdefault("engine", self._engine)
        kwargs.setdefault("max_workers", 5)
        kwargs.setdefault("retry_strategy", NoRetryStrategy())
        kwargs.setdefault("max_fetch_interval", 0.1)
        kwargs.setdefault("min_fetch_interval", 0.01)
        kwargs.setdefault("fetch_batch_size", 5)
        kwargs.setdefault("overfetch_factor", 1)
        kwargs.setdefault("flush_interval", 0.01)
        kwargs.setdefault("release_stuck_interval", 10)
        kwargs.setdefault("release_stuck_timeout", 10)
        kwargs.setdefault("max_deliveries", 20)
        kwargs.setdefault("ack_policy", AckPolicy.NACK_ON_ERROR)

        return args, kwargs
