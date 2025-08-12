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
        cmd = super().patch_command(cmd)
        
        # If it's already a GCPPubSubPublishCommand, just update the topic
        if isinstance(cmd, GCPPubSubPublishCommand):
            cmd.topic = self.topic
            return cmd
        
        # Otherwise, create a new GCPPubSubPublishCommand from the base command
        return GCPPubSubPublishCommand(
            message=cmd.message,
            topic=self.topic,
            attributes=getattr(cmd, 'attributes', None),
            ordering_key=getattr(cmd, 'ordering_key', None),
            correlation_id=cmd.correlation_id,
            _publish_type=cmd._publish_type,
            timeout=getattr(cmd, 'timeout', 30.0),
        )