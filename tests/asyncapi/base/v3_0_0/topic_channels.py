from tests.asyncapi.base.topic_channels import (
    TopicChannelsTestcase as BaseTopicChannelsTestcase,
)

from .basic import AsyncAPI300Factory


class TopicChannelsTestcase(BaseTopicChannelsTestcase, AsyncAPI300Factory):
    pass
