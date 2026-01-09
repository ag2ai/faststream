from datetime import datetime
from typing import Any
from faststream._internal.basic_types import SendableMessage
from faststream.exceptions import FeatureNotSupportedException
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
        next_attempt_at: datetime | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        if next_attempt_at and next_attempt_at.tzinfo is None:
            raise DatetimeMissingTimezoneException

        super().__init__(
            body=message,
            destination=queue,
            headers=headers,
            _publish_type=PublishType.PUBLISH,
        )
        self.next_attempt_at = next_attempt_at
        if self.next_attempt_at:
            self.next_attempt_at = self.next_attempt_at.replace(tzinfo=None)
        self.connection = connection