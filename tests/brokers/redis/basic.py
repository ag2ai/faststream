from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from typing_extensions import override

from faststream.redis import (
    RedisBroker,
    RedisClusterBroker,
    RedisRouter,
    TestRedisBroker,
)
from tests.brokers.base.basic import BaseTestcaseConfig


class RedisTestcaseConfig(BaseTestcaseConfig[RedisBroker]):
    supports_cancel_ack_skip: bool = False

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> RedisBroker:
        return RedisBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> RedisRouter:
        return RedisRouter(**kwargs)


class RedisMemoryTestcaseConfig(RedisTestcaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: RedisBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[RedisBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: RedisBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[RedisBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: RedisBroker,
        **kwargs: Any,
    ) -> Any:
        return TestRedisBroker(*brokers, **kwargs)


class RedisClusterTestcaseConfig(BaseTestcaseConfig[RedisClusterBroker]):
    """Test config for ``RedisClusterBroker``.

    Connects to a real Redis Cluster.
    A single startup node is enough — the cluster auto-discovers the rest.
    """

    supports_cancel_ack_skip: bool = False

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

    def get_router(self, **kwargs: Any) -> RedisRouter:
        return RedisRouter(**kwargs)


class RedisClusterMemoryTestcaseConfig(RedisClusterTestcaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: RedisClusterBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[RedisClusterBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: RedisClusterBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[RedisClusterBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: RedisClusterBroker,
        **kwargs: Any,
    ) -> Any:
        return TestRedisBroker(*brokers, **kwargs)
