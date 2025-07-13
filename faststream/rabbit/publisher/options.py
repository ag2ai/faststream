from typing import TYPE_CHECKING, Optional

from typing_extensions import TypedDict

if TYPE_CHECKING:
    from aio_pika.abc import DateType, HeadersType, TimeoutType


class PublishOptions(TypedDict, total=False):
    mandatory: bool
    immediate: bool
    timeout: "TimeoutType"


class BasicMessageOptions(TypedDict, total=False):
    persist: bool
    content_type: str | None
    content_encoding: str | None
    priority: int | None
    expiration: "DateType"
    message_id: str | None
    timestamp: "DateType"
    message_type: str | None
    user_id: str | None
    app_id: str | None


class MessageOptions(BasicMessageOptions, total=False):
    reply_to: str | None
    headers: Optional["HeadersType"]
    correlation_id: str | None


class PublishKwargs(MessageOptions, PublishOptions, total=False):
    """Typed dict to annotate RabbitMQ publishers."""
