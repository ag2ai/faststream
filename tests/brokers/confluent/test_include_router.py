import pytest

from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)

from .basic import ConfluentTestcaseConfig


@pytest.mark.confluent()
class TestSubscriber(ConfluentTestcaseConfig, IncludeSubscriberTestcase):
    pass


@pytest.mark.confluent()
class TestPublisher(ConfluentTestcaseConfig, IncludePublisherTestcase):
    pass
