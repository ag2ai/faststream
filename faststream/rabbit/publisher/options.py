from typing import TYPE_CHECKING, Optional

from typing_extensions import TypedDict

if TYPE_CHECKING:
    from aio_pika.abc import DateType, HeadersType, TimeoutType


class PublishOptions(TypedDict, total=False):
    mandatory: bool
    immediate: bool
    timeout: "TimeoutType"


class MessageOptions(TypedDict, total=False):
    persist: bool
    reply_to: str | None
    headers: Optional["HeadersType"]
    content_type: str | None
    content_encoding: str | None
    priority: int | None
    expiration: "DateType"
    message_id: str | None
    timestamp: "DateType"
    message_type: str | None
    user_id: str | None
    app_id: str | None
    correlation_id: str | None


class RequestPublishKwargs(MessageOptions, PublishOptions, total=False):
    """Typed dict to annotate RabbitMQ requesters."""


class PublishKwargs(MessageOptions, PublishOptions, total=False):
    """Typed dict to annotate RabbitMQ publishers."""

    reply_to: str | None
