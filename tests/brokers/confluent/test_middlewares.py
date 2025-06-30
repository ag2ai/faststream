import pytest

from tests.brokers.base.middlewares import (
    ExceptionMiddlewareTestcase,
    MiddlewareTestcase,
    MiddlewaresOrderTestcase,
)

from .basic import ConfluentMemoryTestcaseConfig, ConfluentTestcaseConfig


class TestMiddlewaresOrder(ConfluentMemoryTestcaseConfig, MiddlewaresOrderTestcase):
    pass


@pytest.mark.confluent()
class TestMiddlewares(ConfluentTestcaseConfig, MiddlewareTestcase):
    pass


@pytest.mark.confluent()
class TestExceptionMiddlewares(ConfluentTestcaseConfig, ExceptionMiddlewareTestcase):
    pass
