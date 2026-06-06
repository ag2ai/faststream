import pytest

from tests.brokers.base.consume import BrokerRealConsumeTestcase

from .basic import SQSTestcaseConfig


@pytest.mark.connected()
@pytest.mark.sqs()
class TestConsume(SQSTestcaseConfig, BrokerRealConsumeTestcase):
    pass
