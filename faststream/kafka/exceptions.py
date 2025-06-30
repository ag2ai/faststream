from faststream.exceptions import FastStreamException


class BatchBufferOverflowException(FastStreamException):
    """Exception raised when a buffer overflow occurs when adding a new message to the batches."""

    def __init__(self, message_position: int) -> None:
        self.message_position = message_position

    def __str__(self) -> str:
        return (
            "The batch buffer is full. The position of the message"
            f" in the transferred collection at which the overflow occurred: {self.message_position}"
        )
