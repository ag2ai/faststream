from typing import TYPE_CHECKING, Any, Union

from typing_extensions import override

from faststream.mq.helpers.ids import normalize_mq_id
from faststream.response import PublishCommand, Response
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage


class MQResponse(Response):
    def __init__(
        self,
        body: "SendableMessage",
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        reply_to: str = "",
        reply_to_qmgr: str = "",
        priority: int | None = None,
        persistence: bool | None = None,
        expiry: int | None = None,
        message_type: str | None = None,
    ) -> None:
        super().__init__(
            body=body,
            headers=headers,
            correlation_id=correlation_id,
        )
        self.message_id = message_id
        self.reply_to = reply_to
        self.reply_to_qmgr = reply_to_qmgr
        self.priority = priority
        self.persistence = persistence
        self.expiry = expiry
        self.message_type = message_type

    @override
    def as_publish_command(self) -> "MQPublishCommand":
        return MQPublishCommand(
            self.body,
            destination="",
            headers=self.headers,
            correlation_id=self.correlation_id,
            message_id=self.message_id,
            reply_to=self.reply_to,
            reply_to_qmgr=self.reply_to_qmgr,
            priority=self.priority,
            persistence=self.persistence,
            expiry=self.expiry,
            message_type=self.message_type,
            _publish_type=PublishType.PUBLISH,
        )


class MQPublishCommand(PublishCommand):
    def __init__(
        self,
        message: "SendableMessage",
        *,
        destination: str,
        _publish_type: PublishType,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        reply_to: str = "",
        reply_to_qmgr: str = "",
        priority: int | None = None,
        persistence: bool | None = None,
        expiry: int | None = None,
        message_type: str | None = None,
        timeout: float = 5.0,
        native_correlation_id: bytes | None = None,
    ) -> None:
        super().__init__(
            body=message,
            destination=destination,
            correlation_id=normalize_mq_id(
                correlation_id,
                field_name="correlation_id",
            ),
            headers=headers,
            reply_to=reply_to,
            _publish_type=_publish_type,
        )
        self.message_id = normalize_mq_id(message_id, field_name="message_id")
        self.reply_to_qmgr = reply_to_qmgr
        self.priority = priority
        self.persistence = persistence
        self.expiry = expiry
        self.message_type = message_type
        self.timeout = timeout
        self.native_correlation_id = native_correlation_id

    @classmethod
    def from_cmd(
        cls,
        cmd: Union[PublishCommand, "MQPublishCommand"],
    ) -> "MQPublishCommand":
        if isinstance(cmd, MQPublishCommand):
            return cmd

        return cls(
            cmd.body,
            destination=cmd.destination,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )


MQPublishMessage = MQResponse
