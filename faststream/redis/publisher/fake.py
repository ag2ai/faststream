from typing import TYPE_CHECKING, Union

from faststream._internal.publisher.fake import FakePublisher
from faststream.redis.response import RedisPublishCommand

if TYPE_CHECKING:
    from faststream._internal.publisher.proto import ProducerProto
    from faststream.response.response import PublishCommand


class RedisFakePublisher(FakePublisher):
    """Publisher Interface implementation to use as RPC or REPLY TO answer publisher."""

    def __init__(
        self,
        producer: "ProducerProto",
        channel: str,
    ) -> None:
        super().__init__(producer=producer)
        self.channel = channel

    def patch_command(
        self, cmd: Union["PublishCommand", "RedisPublishCommand"]
    ) -> "RedisPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = RedisPublishCommand.from_cmd(cmd)
        real_cmd.destination = self.channel
        return real_cmd
