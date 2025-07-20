import pytest

from tests.brokers.base.middlewares import (
    ExceptionMiddlewareTestcase,
    MiddlewareTestcase,
    MiddlewaresOrderTestcase,
)

from .basic import NatsMemoryTestcaseConfig, NatsTestcaseConfig


class TestMiddlewaresOrder(NatsMemoryTestcaseConfig, MiddlewaresOrderTestcase):
    pass


@pytest.mark.nats()
class TestMiddlewares(NatsTestcaseConfig, MiddlewareTestcase):
    pass


@pytest.mark.nats()
class TestExceptionMiddlewares(NatsTestcaseConfig, ExceptionMiddlewareTestcase):
    pass
