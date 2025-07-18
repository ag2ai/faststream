import pytest

from tests.brokers.base.parser import CustomParserTestcase

from .basic import RabbitTestcaseConfig


@pytest.mark.rabbit()
class TestCustomParser(RabbitTestcaseConfig, CustomParserTestcase):
    pass
