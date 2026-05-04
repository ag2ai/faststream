from typing import Any

from faststream.redis import (
    RedisBroker,
    RedisClusterBroker,
    RedisRouter,
    TestRedisBroker,
)
from tests.brokers.base.basic import BaseTestcaseConfig


class RedisTestcaseConfig(BaseTestcaseConfig):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> RedisBroker:
        return RedisBroker(apply_types=apply_types, **kwargs)

    def patch_broker(self, broker: RedisBroker, **kwargs: Any) -> RedisBroker:
        return broker

    def get_router(self, **kwargs: Any) -> RedisRouter:
        return RedisRouter(**kwargs)


class RedisMemoryTestcaseConfig(RedisTestcaseConfig):
    def patch_broker(self, broker: RedisBroker, **kwargs: Any) -> RedisBroker:
        return TestRedisBroker(broker, **kwargs)


class RedisClusterTestcaseConfig(BaseTestcaseConfig):
    """Test config for ``RedisClusterBroker``.

    Connects to a real Redis Cluster.
    A single startup node is enough — the cluster auto-discovers the rest.
    """

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> RedisClusterBroker:
        return RedisClusterBroker(
            url="redis://127.0.0.1:7001",
            apply_types=apply_types,
            **kwargs,
        )

    def patch_broker(
        self,
        broker: RedisClusterBroker,
        **kwargs: Any,
    ) -> RedisClusterBroker:
        return broker

    def get_router(self, **kwargs: Any) -> RedisRouter:
        return RedisRouter(**kwargs)


class RedisClusterMemoryTestcaseConfig(RedisClusterTestcaseConfig):
    def patch_broker(
        self,
        broker: RedisClusterBroker,
        **kwargs: Any,
    ) -> TestRedisBroker:
        return TestRedisBroker(broker, **kwargs)
