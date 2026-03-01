import pytest

from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import RabbitTestcaseConfig


@pytest.mark.rabbit()
class TestSubscriber(RabbitTestcaseConfig, IncludeSubscriberTestcase):
    pass


@pytest.mark.rabbit()
class TestPublisher(RabbitTestcaseConfig, IncludePublisherTestcase):
    pass
