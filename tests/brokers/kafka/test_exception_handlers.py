import pytest

from tests.brokers.base.exception_handlers import ExceptionHandlersTestcase

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
class TestExceptionHandlers(KafkaTestcaseConfig, ExceptionHandlersTestcase):
    pass
