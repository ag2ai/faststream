from typing import Any

from faststream.redis import RedisBroker, RedisRouter, TestRedisBroker
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
