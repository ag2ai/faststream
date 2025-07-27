import pytest

from tests.brokers.base.middlewares import (
    ExceptionMiddlewareTestcase,
    MiddlewareTestcase,
    MiddlewaresOrderTestcase,
)

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig


@pytest.mark.redis()
class TestMiddlewaresOrder(RedisMemoryTestcaseConfig, MiddlewaresOrderTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.redis()
class TestMiddlewares(RedisTestcaseConfig, MiddlewareTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.redis()
class TestExceptionMiddlewares(RedisTestcaseConfig, ExceptionMiddlewareTestcase):
    pass
