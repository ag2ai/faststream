import pytest

from faststream.kafka import KafkaBroker
from tests.asyncapi.base.v3_0_0.topic_channels import TopicChannelsTestcase


@pytest.mark.kafka()
class TestTopicChannels(TopicChannelsTestcase):
    broker_class = KafkaBroker
