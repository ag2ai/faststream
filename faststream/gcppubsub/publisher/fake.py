"""GCP Pub/Sub fake publisher for testing and response publishing."""

from typing import TYPE_CHECKING, Union

from faststream._internal.endpoint.publisher.fake import FakePublisher
from faststream.gcppubsub.response import GCPPubSubPublishCommand

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto
    from faststream.response.response import PublishCommand


class GCPPubSubFakePublisher(FakePublisher):
    """Publisher Interface implementation to use as RPC or REPLY TO answer publisher."""

    def __init__(
        self,
        producer: "ProducerProto[GCPPubSubPublishCommand]",
        topic: str,
    ) -> None:
        super().__init__(producer=producer)
        self.topic = topic

    def patch_command(
        self,
        cmd: Union["PublishCommand", "GCPPubSubPublishCommand"],
    ) -> "GCPPubSubPublishCommand":
        # If it's already a GCPPubSubPublishCommand, just update the topic
        if isinstance(cmd, GCPPubSubPublishCommand):
            cmd.topic = self.topic
            return cmd

        # Otherwise, create a new GCPPubSubPublishCommand from the base command
        return GCPPubSubPublishCommand(
            message=cmd.body,  # Use body instead of message
            topic=self.topic,
            attributes=cmd.headers if isinstance(cmd.headers, dict) else {},
            correlation_id=cmd.correlation_id,
            _publish_type=cmd.publish_type,
            timeout=30.0,
        )
