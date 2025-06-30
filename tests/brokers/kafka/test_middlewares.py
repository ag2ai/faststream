import pytest

from tests.brokers.base.middlewares import (
    ExceptionMiddlewareTestcase,
    MiddlewareTestcase,
    MiddlewaresOrderTestcase,
)

from .basic import KafkaMemoryTestcaseConfig, KafkaTestcaseConfig


class TestMiddlewaresOrder(KafkaMemoryTestcaseConfig, MiddlewaresOrderTestcase):
    pass


@pytest.mark.kafka()
class TestMiddlewares(KafkaTestcaseConfig, MiddlewareTestcase):
    pass


@pytest.mark.kafka()
class TestExceptionMiddlewares(KafkaTestcaseConfig, ExceptionMiddlewareTestcase):
    pass
