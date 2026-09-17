from tests.asyncapi.base.topic_channels import (
    TopicChannelsTestcase as BaseTopicChannelsTestcase,
)

from .basic import AsyncAPI260Factory


class TopicChannelsTestcase(BaseTopicChannelsTestcase, AsyncAPI260Factory):
    pass
