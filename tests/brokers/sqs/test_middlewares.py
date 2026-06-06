import pytest

from tests.brokers.base.middlewares import (
    ExceptionMiddlewareTestcase,
    MiddlewareTestcase,
    MiddlewaresOrderTestcase,
)

from .basic import SQSMemoryTestcaseConfig, SQSTestcaseConfig


@pytest.mark.sqs()
class TestMiddlewaresOrder(SQSMemoryTestcaseConfig, MiddlewaresOrderTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.sqs()
class TestMiddlewares(SQSTestcaseConfig, MiddlewareTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.sqs()
class TestExceptionMiddlewares(SQSTestcaseConfig, ExceptionMiddlewareTestcase):
    pass
