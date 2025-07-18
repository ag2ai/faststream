from typing import TYPE_CHECKING, Union

from faststream._internal.endpoint.publisher.fake import FakePublisher
from faststream.nats.response import NatsPublishCommand

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto
    from faststream.response.response import PublishCommand


class NatsFakePublisher(FakePublisher):
    """Publisher Interface implementation to use as RPC or REPLY TO answer publisher."""

    def __init__(
        self,
        producer: "ProducerProto[NatsPublishCommand]",
        subject: str,
    ) -> None:
        super().__init__(producer=producer)
        self.subject = subject

    def patch_command(
        self,
        cmd: Union["PublishCommand", "NatsPublishCommand"],
    ) -> "NatsPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = NatsPublishCommand.from_cmd(cmd)
        real_cmd.destination = self.subject
        return real_cmd
