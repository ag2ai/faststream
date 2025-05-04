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
    reply_to: Optional[str]
    headers: Optional["HeadersType"]
    content_type: Optional[str]
    content_encoding: Optional[str]
    priority: Optional[int]
    expiration: "DateType"
    message_id: Optional[str]
    timestamp: "DateType"
    message_type: Optional[str]
    user_id: Optional[str]
    app_id: Optional[str]
    correlation_id: Optional[str]
