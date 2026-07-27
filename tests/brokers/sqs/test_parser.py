import pytest

from tests.brokers.base.parser import CustomParserTestcase

from .basic import SQSTestcaseConfig


@pytest.mark.sqs()
@pytest.mark.connected()
class TestCustomParser(SQSTestcaseConfig, CustomParserTestcase):
    pass
