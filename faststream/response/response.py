from collections.abc import Sequence
from typing import Any

from typing_extensions import Self

from .publish_type import PublishType


class Response:
    def __init__(
        self,
        body: Any,
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize a handler."""
        self.body = body
        self.headers = headers or {}
        self.correlation_id = correlation_id

    def as_publish_command(self) -> "PublishCommand":
        """Method to transform handlers' Response result to DTO for publishers."""
        return PublishCommand(
            body=self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            _publish_type=PublishType.PUBLISH,
        )


class PublishCommand(Response):
    def __init__(
        self,
        body: Any,
        *,
        _publish_type: PublishType,
        reply_to: str = "",
        destination: str = "",
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            body,
            headers=headers,
            correlation_id=correlation_id,
        )

        self.destination = destination
        self.reply_to = reply_to

        self.publish_type = _publish_type

    @property
    def batch_bodies(self) -> tuple["Any", ...]:
        if self.body is not None:
            return (self.body,)
        return ()

    def add_headers(
        self,
        headers: dict[str, Any],
        *,
        override: bool = True,
    ) -> None:
        if override:
            self.headers |= headers
        else:
            self.headers = headers | self.headers

    @classmethod
    def from_cmd(cls, cmd: Self) -> Self:
        raise NotImplementedError


class BatchPublishCommand(PublishCommand):
    def __init__(
        self,
        body: Any,
        /,
        *bodies: Any,
        _publish_type: PublishType,
        reply_to: str = "",
        destination: str = "",
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            body,
            headers=headers,
            correlation_id=correlation_id,
            destination=destination,
            reply_to=reply_to,
            _publish_type=_publish_type,
        )
        self.extra_bodies = bodies

    @property
    def batch_bodies(self) -> tuple["Any", ...]:
        return (*super().batch_bodies, *self.extra_bodies)

    @batch_bodies.setter
    def batch_bodies(self, value: Sequence["Any"]) -> None:
        if len(value) == 0:
            self.body = None
            self.extra_bodies = ()
        else:
            self.body = value[0]
            self.extra_bodies = tuple(value[1:])

    @classmethod
    def from_cmd(
        cls,
        cmd: "PublishCommand",
        *,
        batch: bool = False,
    ) -> "BatchPublishCommand":
        raise NotImplementedError

    @staticmethod
    def _parse_bodies(body: Any, *, batch: bool = False) -> tuple[Any, tuple[Any, ...]]:
        extra_bodies = []
        if batch and isinstance(body, Sequence) and not isinstance(body, (str, bytes)):
            if body:
                body, extra_bodies = body[0], body[1:]
            else:
                body = None
        return body, tuple(extra_bodies)
