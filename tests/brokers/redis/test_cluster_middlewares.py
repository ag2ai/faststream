import pytest

from tests.brokers.base.middlewares import (
    ExceptionMiddlewareTestcase,
    MiddlewareTestcase,
    MiddlewaresOrderTestcase,
)

from .basic import RedisClusterMemoryTestcaseConfig, RedisClusterTestcaseConfig


@pytest.mark.redis_cluster()
class TestClusterMiddlewaresOrder(
    RedisClusterMemoryTestcaseConfig, MiddlewaresOrderTestcase
):
    pass


@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterMiddlewares(RedisClusterTestcaseConfig, MiddlewareTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.redis_cluster()
class TestClusterExceptionMiddlewares(
    RedisClusterTestcaseConfig, ExceptionMiddlewareTestcase
):
    pass
