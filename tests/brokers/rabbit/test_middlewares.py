import pytest

from tests.brokers.base.middlewares import (
    ExceptionMiddlewareTestcase,
    MiddlewareTestcase,
    MiddlewaresOrderTestcase,
)

from .basic import RabbitMemoryTestcaseConfig, RabbitTestcaseConfig


@pytest.mark.rabbit()
class TestMiddlewaresOrder(RabbitMemoryTestcaseConfig, MiddlewaresOrderTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestMiddlewares(RabbitTestcaseConfig, MiddlewareTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestExceptionMiddlewares(RabbitTestcaseConfig, ExceptionMiddlewareTestcase):
    pass
