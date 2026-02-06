from datetime import datetime
from typing import Union
from faststream._internal.basic_types import SendableMessage
from faststream.response.publish_type import PublishType
from faststream.response.response import PublishCommand
from sqlalchemy.ext.asyncio import AsyncConnection

from faststream.sqla.exceptions import DatetimeMissingTimezoneException


class SqlaPublishCommand(PublishCommand):
    def __init__(
        self,
        message: "SendableMessage",
        *,
        queue: str,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        if next_attempt_at and next_attempt_at.tzinfo is None:
            raise DatetimeMissingTimezoneException

        super().__init__(
            body=message,
            destination=queue,
            headers=headers,
            correlation_id=correlation_id,
            _publish_type=PublishType.PUBLISH,
        )
        self.next_attempt_at = next_attempt_at
        if self.next_attempt_at:
            self.next_attempt_at = self.next_attempt_at.replace(tzinfo=None)
        self.connection = connection

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "SqlaPublishCommand"],
    ) -> "SqlaPublishCommand":
        if isinstance(cmd, SqlaPublishCommand):
            return cmd

        return cls(
            cmd.body,
            queue=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
        )

    def headers_to_publish(self) -> dict[str, str]:
        headers = {}

        if self.correlation_id:
            headers["correlation_id"] = self.correlation_id

        return headers | (self.headers or {})
