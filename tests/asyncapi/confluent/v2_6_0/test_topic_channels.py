import pytest

from faststream.confluent import KafkaBroker
from tests.asyncapi.base.v2_6_0.topic_channels import TopicChannelsTestcase


@pytest.mark.confluent()
class TestTopicChannels(TopicChannelsTestcase):
    broker_class = KafkaBroker
