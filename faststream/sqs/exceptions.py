from typing import Any

from faststream.exceptions import FastStreamException

# AWS SQS hard limits (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html)
MAX_BATCH_ENTRIES = 10
MAX_MESSAGE_ATTRIBUTES = 10
MAX_MESSAGE_SIZE = 262_144  # 256 KiB, shared by body + attributes


class SQSError(FastStreamException):
    """Base class for SQS-specific FastStream errors."""


class MessageTooLargeError(SQSError, ValueError):
    """Raised when a message body (with attributes) exceeds the 256 KiB SQS limit."""

    def __init__(self, size: int) -> None:
        self.size = size

    def __str__(self) -> str:
        return (
            f"Message size {self.size} bytes exceeds the SQS limit of "
            f"{MAX_MESSAGE_SIZE} bytes (256 KiB)."
        )


class TooManyMessageAttributesError(SQSError, ValueError):
    """Raised when a message carries more than 10 ``MessageAttributes``.

    SQS reserves a maximum of 10 message attributes per message. FastStream
    transports ``content-type``/``reply_to``/``correlation_id`` as attributes,
    so a few slots are always consumed by transport metadata.
    """

    def __init__(self, count: int) -> None:
        self.count = count

    def __str__(self) -> str:
        return (
            f"A message can carry at most {MAX_MESSAGE_ATTRIBUTES} MessageAttributes, "
            f"got {self.count} (including FastStream transport attributes "
            "content-type/reply_to/correlation_id). Reduce the number of headers."
        )


class FifoQueueError(SQSError, ValueError):
    """Raised on invalid FIFO publish parameters (missing group/deduplication id)."""


class BatchSendError(SQSError):
    """Raised when ``send_message_batch`` reports failed entries.

    ``failed`` holds the raw ``Failed`` records returned by SQS so callers can
    inspect per-message error codes.
    """

    def __init__(self, failed: list[dict[str, Any]]) -> None:
        self.failed = failed

    def __str__(self) -> str:
        ids = ", ".join(f.get("Id", "?") for f in self.failed)
        return f"{len(self.failed)} message(s) failed to send in batch: ids={ids}"
