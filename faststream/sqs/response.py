from typing import TYPE_CHECKING, Union

from typing_extensions import override

from faststream.response.publish_type import PublishType
from faststream.response.response import (
    BatchPublishCommand,
    PublishCommand,
    Response,
)

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage


class SQSResponse(Response):
    def __init__(
        self,
        body: "SendableMessage",
        *,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        delay_seconds: int = 0,
        group_id: str | None = None,
        deduplication_id: str | None = None,
    ) -> None:
        super().__init__(body=body, headers=headers, correlation_id=correlation_id)
        self.delay_seconds = delay_seconds
        self.group_id = group_id
        self.deduplication_id = deduplication_id

    @override
    def as_publish_command(self) -> "SQSPublishCommand":
        return SQSPublishCommand(
            self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            _publish_type=PublishType.PUBLISH,
            queue="",
            delay_seconds=self.delay_seconds,
            group_id=self.group_id,
            deduplication_id=self.deduplication_id,
        )


class SQSPublishCommand(PublishCommand):
    def __init__(
        self,
        message: "SendableMessage",
        *,
        queue: str = "",
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        delay_seconds: int = 0,
        group_id: str | None = None,
        deduplication_id: str | None = None,
        timeout: float | None = 30.0,
        _publish_type: PublishType,
    ) -> None:
        super().__init__(
            body=message,
            destination=queue,
            correlation_id=correlation_id,
            headers=headers,
            reply_to=reply_to,
            _publish_type=_publish_type,
        )
        self.delay_seconds = delay_seconds
        self.group_id = group_id
        self.deduplication_id = deduplication_id
        self.timeout = timeout

    @property
    def queue(self) -> str:
        return self.destination

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "SQSPublishCommand"],
    ) -> "SQSPublishCommand":
        if isinstance(cmd, SQSPublishCommand):
            return cmd

        return cls(
            cmd.body,
            queue=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            timeout=getattr(cmd, "timeout", None),
            _publish_type=cmd.publish_type,
        )

    def __repr__(self) -> str:
        body: list[str] = [
            f"body='{self.body}'",
            f"queue='{self.destination}'",
        ]
        if self.group_id:
            body.append(f"group_id='{self.group_id}'")
        if self.reply_to:
            body.append(f"reply_to='{self.reply_to}'")
        body.extend((
            f"headers={self.headers}",
            f"correlation_id='{self.correlation_id}'",
        ))
        return f"{self.__class__.__name__}({', '.join(body)})"


class SQSBatchPublishCommand(BatchPublishCommand):
    def __init__(
        self,
        body: "SendableMessage",
        /,
        *bodies: "SendableMessage",
        queue: str = "",
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        group_id: str | None = None,
        deduplication_id: str | None = None,
        _publish_type: PublishType = PublishType.PUBLISH,
    ) -> None:
        super().__init__(
            body,
            *bodies,
            destination=queue,
            correlation_id=correlation_id,
            headers=headers,
            reply_to=reply_to,
            _publish_type=_publish_type,
        )
        self.group_id = group_id
        self.deduplication_id = deduplication_id
        self.delay_seconds = 0

    @property
    def queue(self) -> str:
        return self.destination

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "SQSBatchPublishCommand"],
        *,
        batch: bool = False,
    ) -> "SQSBatchPublishCommand":
        if isinstance(cmd, SQSBatchPublishCommand):
            return cmd

        body, extra_bodies = cls._parse_bodies(cmd.body, batch=batch)
        return cls(
            body,
            *extra_bodies,
            queue=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )
