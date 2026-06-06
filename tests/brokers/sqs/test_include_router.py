import pytest

from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import SQSTestcaseConfig


@pytest.mark.sqs()
class TestSubscriber(SQSTestcaseConfig, IncludeSubscriberTestcase):
    pass


@pytest.mark.sqs()
class TestPublisher(SQSTestcaseConfig, IncludePublisherTestcase):
    pass
