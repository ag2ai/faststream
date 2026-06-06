import pytest

from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import SQSTestcaseConfig


@pytest.mark.connected()
@pytest.mark.sqs()
class TestPublish(SQSTestcaseConfig, BrokerPublishTestcase):
    pass
