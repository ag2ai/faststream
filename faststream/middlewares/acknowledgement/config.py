from enum import Enum


class AckPolicy(str, Enum):
    ACK_FIRST = "ack_first"
    """Ack message on consume."""

    ACK = "ack"
    """Ack message after all process."""

    REJECT_ON_ERROR = "reject_on_error"
    """Reject message on unhandled exceptions."""

    NACK_ON_ERROR = "nack_on_error"
    """Nack message on unhandled exceptions."""

    MANUAL = "manual"
    """Disable default FastStream Acknowledgement logic. User should confirm all actions manually."""
