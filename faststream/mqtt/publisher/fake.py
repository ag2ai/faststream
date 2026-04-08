from typing import TYPE_CHECKING, Union

from faststream._internal.endpoint.publisher.fake import FakePublisher
from faststream.mqtt.response import MQTTPublishCommand

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto
    from faststream.response.response import PublishCommand


class MQTTFakePublisher(FakePublisher):
    """Publisher used for RPC / reply-to responses in MQTT."""

    def __init__(
        self,
        producer: "ProducerProto[MQTTPublishCommand]",
        topic: str,
    ) -> None:
        super().__init__(producer=producer)
        self.topic = topic

    def patch_command(
        self,
        cmd: Union["PublishCommand", "MQTTPublishCommand"],
    ) -> "MQTTPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = MQTTPublishCommand.from_cmd(cmd)
        real_cmd.destination = self.topic
        return real_cmd
