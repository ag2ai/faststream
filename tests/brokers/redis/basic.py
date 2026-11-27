from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from typing_extensions import override

from faststream.redis import (
    RedisBroker,
    RedisClusterBroker,
    RedisRouter,
    StreamSub,
    TestRedisBroker,
)
from tests.brokers.base.basic import BaseTestcaseConfig


class RedisTestcaseConfig(BaseTestcaseConfig[RedisBroker]):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> RedisBroker:
        return RedisBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> RedisRouter:
        return RedisRouter(**kwargs)

    @override
    def get_ack_subscriber_params(
        self,
        queue: str,
        **kwargs: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        # A channel subscriber forces MANUAL ack, so ack needs a consumer group.
        return (), {"stream": StreamSub(queue, group=queue, consumer=queue), **kwargs}

    @override
    async def publish_to_ack_subscriber(
        self,
        broker: Any,
        queue: str,
        message: str,
    ) -> None:
        await broker.publish(message, stream=queue)


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

    @override
    def get_ack_subscriber_params(
        self,
        queue: str,
        **kwargs: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        # A channel subscriber forces MANUAL ack, so ack needs a consumer group.
        return (), {"stream": StreamSub(queue, group=queue, consumer=queue), **kwargs}

    @override
    async def publish_to_ack_subscriber(
        self,
        broker: Any,
        queue: str,
        message: str,
    ) -> None:
        await broker.publish(message, stream=queue)


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
