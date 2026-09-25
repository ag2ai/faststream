from typing import TYPE_CHECKING, Optional, Union

from typing_extensions import override

from faststream.response.publish_type import PublishType
from faststream.response.response import PublishCommand, Response

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream.nats.schemas.schedule import Schedule


class NatsResponse(Response):
    def __init__(
        self,
        body: "SendableMessage",
        *,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        stream: str | None = None,
        schedule: Optional["Schedule"] = None,
    ) -> None:
        super().__init__(
            body=body,
            headers=headers,
            correlation_id=correlation_id,
        )
        self.stream = stream
        self.schedule = schedule

    @override
    def as_publish_command(self) -> "NatsPublishCommand":
        return NatsPublishCommand(
            message=self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            _publish_type=PublishType.PUBLISH,
            # Nats specific
            subject="",
            stream=self.stream,
            schedule=self.schedule,
        )


class NatsPublishCommand(PublishCommand):
    def __init__(
        self,
        message: "SendableMessage",
        *,
        subject: str = "",
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        stream: str | None = None,
        timeout: float = 0.5,
        schedule: Optional["Schedule"] = None,
        _publish_type: PublishType,
    ) -> None:
        super().__init__(
            body=message,
            destination=subject,
            correlation_id=correlation_id,
            headers=headers,
            reply_to=reply_to,
            _publish_type=_publish_type,
        )

        self.stream = stream
        self.timeout = timeout
        self.schedule = schedule

    def headers_to_publish(self, *, js: bool = False) -> dict[str, str]:
        headers = {}

        if self.correlation_id:
            headers["correlation_id"] = self.correlation_id

        if js and self.reply_to:
            headers["reply_to"] = self.reply_to

        if self.schedule:
            headers["Nats-Schedule"] = f"@at {self.schedule.time.isoformat()}"
            headers["Nats-Schedule-Target"] = self.schedule.target

        return headers | self.headers

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "NatsPublishCommand"],
    ) -> "NatsPublishCommand":
        if isinstance(cmd, NatsPublishCommand):
            # NOTE: Should return a copy probably.
            return cmd

        return cls(
            message=cmd.body,
            subject=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )

    def __repr__(self) -> str:
        body = [f"body='{self.body}'", f"subject='{self.destination}'"]
        if self.stream:
            body.append(f"stream={self.stream}")
        if self.reply_to:
            body.append(f"reply_to='{self.reply_to}'")
        body.extend((
            f"headers={self.headers}",
            f"correlation_id='{self.correlation_id}'",
            f"publish_type={self.publish_type}",
        ))
        return f"{self.__class__.__name__}({', '.join(body)})"
