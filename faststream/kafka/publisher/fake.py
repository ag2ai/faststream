from typing import TYPE_CHECKING, Union

from faststream._internal.endpoint.publisher.fake import FakePublisher
from faststream.kafka.response import KafkaPublishCommand

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto
    from faststream.response.response import PublishCommand


class KafkaFakePublisher(FakePublisher):
    """Publisher Interface implementation to use as RPC or REPLY TO answer publisher."""

    def __init__(
        self,
        producer: "ProducerProto[KafkaPublishCommand]",
        topic: str,
    ) -> None:
        super().__init__(producer=producer)
        self.topic = topic

    def patch_command(
        self,
        cmd: Union["PublishCommand", "KafkaPublishCommand"],
    ) -> "KafkaPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = KafkaPublishCommand.from_cmd(cmd)
        real_cmd.destination = self.topic
        return real_cmd
