import pytest

from tests.brokers.base.middlewares import (
    ExceptionMiddlewareTestcase,
    MiddlewareTestcase,
    MiddlewaresOrderTestcase,
)

from .basic import NatsMemoryTestcaseConfig, NatsTestcaseConfig


@pytest.mark.nats()
class TestMiddlewaresOrder(NatsMemoryTestcaseConfig, MiddlewaresOrderTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.nats()
class TestMiddlewares(NatsTestcaseConfig, MiddlewareTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.nats()
class TestExceptionMiddlewares(NatsTestcaseConfig, ExceptionMiddlewareTestcase):
    pass
