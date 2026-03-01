from typing import TYPE_CHECKING, Union

from faststream._internal.endpoint.publisher.fake import FakePublisher
from faststream.redis.response import RedisPublishCommand

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto
    from faststream.redis.parser import MessageFormat
    from faststream.response.response import PublishCommand


class RedisFakePublisher(FakePublisher):
    """Publisher Interface implementation to use as RPC or REPLY TO answer publisher."""

    def __init__(
        self,
        producer: "ProducerProto[RedisPublishCommand]",
        channel: str,
        message_format: type["MessageFormat"],
    ) -> None:
        super().__init__(producer=producer)
        self.channel = channel
        self.message_format = message_format

    def patch_command(
        self,
        cmd: Union["PublishCommand", "RedisPublishCommand"],
    ) -> "RedisPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = RedisPublishCommand.from_cmd(cmd, message_format=self.message_format)
        real_cmd.destination = self.channel
        return real_cmd
