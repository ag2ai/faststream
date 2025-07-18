import pytest

from tests.brokers.base.parser import CustomParserTestcase

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
class TestCustomParser(KafkaTestcaseConfig, CustomParserTestcase):
    pass
