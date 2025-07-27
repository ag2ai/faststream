import pytest

from tests.brokers.base.parser import CustomParserTestcase

from .basic import ConfluentTestcaseConfig


@pytest.mark.connected()
@pytest.mark.confluent()
class TestCustomParser(ConfluentTestcaseConfig, CustomParserTestcase):
    pass
