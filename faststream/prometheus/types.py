from enum import StrEnum
from typing import TypedDict


class ProcessingStatus(StrEnum):
    acked = "acked"
    nacked = "nacked"
    rejected = "rejected"
    skipped = "skipped"
    error = "error"


class PublishingStatus(StrEnum):
    success = "success"
    error = "error"


class ConsumeAttrs(TypedDict):
    message_size: int
    destination_name: str
    messages_count: int
