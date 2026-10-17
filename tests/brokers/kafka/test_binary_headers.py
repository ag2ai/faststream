import pytest

from tests.brokers.base.header import BinaryHeadersTestcase

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
class TestBinaryHeaders(KafkaMemoryTestcaseConfig, BinaryHeadersTestcase):
    pass
