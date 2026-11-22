import pytest

from tests.brokers.base.header import (
    BinaryHeadersPublishTestcase,
    BinaryHeadersTestcase,
)

from .basic import ConfluentMemoryTestcaseConfig, ConfluentTestcaseConfig


@pytest.mark.confluent()
class TestBinaryHeaders(ConfluentMemoryTestcaseConfig, BinaryHeadersTestcase):
    pass


@pytest.mark.confluent()
@pytest.mark.connected()
class TestBinaryHeadersConnected(ConfluentTestcaseConfig, BinaryHeadersPublishTestcase):
    pass
