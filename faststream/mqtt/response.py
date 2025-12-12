from typing import TYPE_CHECKING, Any

from typing_extensions import override

from faststream.response import PublishCommand, PublishType
from faststream.response.response import Response

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage


class MQTTResponse(Response):
    def __init__(
        self,
        body: "SendableMessage",
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(body, headers=headers, correlation_id=correlation_id)

    @override
    def as_publish_command(self) -> "MQTTPublishCommand":
        return MQTTPublishCommand(
            self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            _publish_type=PublishType.PUBLISH,
        )


class MQTTPublishCommand(PublishCommand):
    @classmethod
    def from_cmd(
        cls,
        cmd: "PublishCommand | MQTTPublishCommand",
        *,
        batch: bool = False,
    ) -> "MQTTPublishCommand":
        return cls(
            cmd.body,
            _publish_type=cmd.publish_type,
            reply_to=cmd.reply_to,
            destination=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
        )
