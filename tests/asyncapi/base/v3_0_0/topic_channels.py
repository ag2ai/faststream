from dirty_equals import IsPartialDict

from tests.asyncapi.base.topic_channels import (
    TopicChannelsTestcase as BaseTopicChannelsTestcase,
)

from .basic import AsyncAPI300Factory


class TopicChannelsTestcase(BaseTopicChannelsTestcase, AsyncAPI300Factory):
    def test_a_title_leaves_every_channel_operated(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("first", "second", title="Titled")
        async def handle() -> None: ...

        # the title used to key the operations too, leaving all but one channel
        # referenced by nothing
        assert self.get_spec(broker).to_jsonable()["operations"] == IsPartialDict(
            {
                "Titled:firstSubscribe": IsPartialDict(
                    channel={"$ref": "#/channels/Titled:first"},
                ),
                "Titled:secondSubscribe": IsPartialDict(
                    channel={"$ref": "#/channels/Titled:second"},
                ),
            },
        )
