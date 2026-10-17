import pytest

from tests.brokers.base.header import BinaryHeadersTestcase

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
class TestBinaryHeaders(ConfluentMemoryTestcaseConfig, BinaryHeadersTestcase):
    pass
