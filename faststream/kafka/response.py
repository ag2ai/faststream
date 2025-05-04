from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import override

from faststream.response.publish_type import PublishType
from faststream.response.response import BatchPublishCommand, PublishCommand, Response

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict, SendableMessage


class KafkaResponse(Response):
    def __init__(
        self,
        body: "SendableMessage",
        *,
        headers: Optional["AnyDict"] = None,
        correlation_id: Optional[str] = None,
        timestamp_ms: Optional[int] = None,
        key: Optional[bytes] = None,
    ) -> None:
        super().__init__(
            body=body,
            headers=headers,
            correlation_id=correlation_id,
        )

        self.timestamp_ms = timestamp_ms
        self.key = key

    @override
    def as_publish_command(self) -> "KafkaPublishCommand":
        return KafkaPublishCommand(
            self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            _publish_type=PublishType.PUBLISH,
            # Kafka specific
            topic="",
            key=self.key,
            timestamp_ms=self.timestamp_ms,
        )


class KafkaPublishCommand(BatchPublishCommand):
    def __init__(
        self,
        message: "SendableMessage",
        /,
        *messages: "SendableMessage",
        topic: str,
        _publish_type: PublishType,
        key: Union[bytes, Any, None] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        reply_to: str = "",
        no_confirm: bool = False,
        timeout: float = 0.5,
    ) -> None:
        super().__init__(
            message,
            *messages,
            destination=topic,
            reply_to=reply_to,
            correlation_id=correlation_id,
            headers=headers,
            _publish_type=_publish_type,
        )

        self.key = key
        self.partition = partition
        self.timestamp_ms = timestamp_ms
        self.no_confirm = no_confirm

        # request option
        self.timeout = timeout

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "KafkaPublishCommand"],
        *,
        batch: bool = False,
    ) -> "KafkaPublishCommand":
        if isinstance(cmd, KafkaPublishCommand):
            # NOTE: Should return a copy probably.
            return cmd

        body, extra_bodies = cls._parse_bodies(cmd.body, batch=batch)

        return cls(
            body,
            *extra_bodies,
            topic=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )

    def headers_to_publish(self) -> dict[str, str]:
        headers = {}

        if self.correlation_id:
            headers["correlation_id"] = self.correlation_id

        if self.reply_to:
            headers["reply_to"] = self.reply_to

        return headers | self.headers
