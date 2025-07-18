from typing import TYPE_CHECKING, Any, Union

from faststream._internal.endpoint.publisher.fake import FakePublisher
from faststream.rabbit.response import RabbitPublishCommand
from faststream.rabbit.schemas import RabbitExchange

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto
    from faststream.response.response import PublishCommand


class RabbitFakePublisher(FakePublisher):
    """Publisher Interface implementation to use as RPC or REPLY TO answer publisher."""

    def __init__(
        self,
        producer: "ProducerProto[Any]",
        routing_key: str,
        app_id: str | None,
        exchange: RabbitExchange,
    ) -> None:
        super().__init__(producer=producer)
        self.routing_key = routing_key
        self.exchange = exchange
        self.app_id = app_id

    def patch_command(
        self,
        cmd: Union["PublishCommand", "RabbitPublishCommand"],
    ) -> "RabbitPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = RabbitPublishCommand.from_cmd(cmd)
        real_cmd.destination = self.routing_key
        real_cmd.exchange = self.exchange
        if self.app_id:
            real_cmd.message_options["app_id"] = self.app_id
        return real_cmd
