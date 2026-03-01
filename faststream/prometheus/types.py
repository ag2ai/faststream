from enum import Enum
from typing import TypedDict


class ProcessingStatus(str, Enum):
    acked = "acked"
    nacked = "nacked"
    rejected = "rejected"
    skipped = "skipped"
    error = "error"


class PublishingStatus(str, Enum):
    success = "success"
    error = "error"


class ConsumeAttrs(TypedDict):
    message_size: int
    destination_name: str
    messages_count: int
