import pytest

from tests.brokers.base.parser import CustomParserTestcase

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
class TestCustomParser(KafkaTestcaseConfig, CustomParserTestcase):
    pass
