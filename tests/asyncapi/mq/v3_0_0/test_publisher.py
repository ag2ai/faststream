import pytest

from faststream.mq import MQBroker
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


@pytest.mark.mq()
class TestPublisher(PublisherTestcase):
    broker_class = MQBroker
