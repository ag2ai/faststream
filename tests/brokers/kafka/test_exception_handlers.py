import pytest

from tests.brokers.base.exception_handlers import ExceptionHandlersTestcase

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
class TestExceptionHandlers(KafkaMemoryTestcaseConfig, ExceptionHandlersTestcase): ...
