from typing import Any

from faststream.redis.prometheus import RedisPrometheusMiddleware
from faststream.redis.prometheus.provider import (
    BatchRedisMetricsSettingsProvider,
    RedisMetricsSettingsProvider,
)
from tests.brokers.redis.basic import RedisTestcaseConfig


class BaseRedisPrometheusSettings(RedisTestcaseConfig):
    messaging_system = "redis"

    def get_middleware(self, **kwargs: Any) -> RedisPrometheusMiddleware:
        return RedisPrometheusMiddleware(**kwargs)


class RedisPrometheusSettings(BaseRedisPrometheusSettings):
    def get_settings_provider(self) -> RedisMetricsSettingsProvider:
        return RedisMetricsSettingsProvider()


class BatchRedisPrometheusSettings(BaseRedisPrometheusSettings):
    def get_settings_provider(self) -> BatchRedisMetricsSettingsProvider:
        return BatchRedisMetricsSettingsProvider()
