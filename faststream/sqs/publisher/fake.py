from typing import TYPE_CHECKING, Union

from faststream._internal.endpoint.publisher.fake import FakePublisher
from faststream.sqs.response import SQSPublishCommand

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto
    from faststream.response.response import PublishCommand


class SQSFakePublisher(FakePublisher):
    """Publisher used for RPC / reply-to responses in SQS."""

    def __init__(
        self,
        producer: "ProducerProto[SQSPublishCommand]",
        queue: str,
    ) -> None:
        super().__init__(producer=producer)
        self.queue = queue

    def patch_command(
        self,
        cmd: Union["PublishCommand", "SQSPublishCommand"],
    ) -> "SQSPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = SQSPublishCommand.from_cmd(cmd)
        real_cmd.destination = self.queue
        return real_cmd
