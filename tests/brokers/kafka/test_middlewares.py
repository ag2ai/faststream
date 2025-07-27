import pytest

from tests.brokers.base.middlewares import (
    ExceptionMiddlewareTestcase,
    MiddlewareTestcase,
    MiddlewaresOrderTestcase,
)

from .basic import KafkaMemoryTestcaseConfig, KafkaTestcaseConfig


@pytest.mark.kafka()
class TestMiddlewaresOrder(KafkaMemoryTestcaseConfig, MiddlewaresOrderTestcase):
    pass


@pytest.mark.kafka()
@pytest.mark.connected()
class TestMiddlewares(KafkaTestcaseConfig, MiddlewareTestcase):
    pass


@pytest.mark.kafka()
@pytest.mark.connected()
class TestExceptionMiddlewares(KafkaTestcaseConfig, ExceptionMiddlewareTestcase):
    pass
