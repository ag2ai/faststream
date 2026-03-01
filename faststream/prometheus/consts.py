from faststream.exceptions import AckMessage, NackMessage, RejectMessage, SkipMessage
from faststream.message.message import AckStatus
from faststream.prometheus.types import ProcessingStatus

PROCESSING_STATUS_BY_HANDLER_EXCEPTION_MAP = {
    AckMessage: ProcessingStatus.acked,
    NackMessage: ProcessingStatus.nacked,
    RejectMessage: ProcessingStatus.rejected,
    SkipMessage: ProcessingStatus.skipped,
}


PROCESSING_STATUS_BY_ACK_STATUS = {
    AckStatus.ACKED: ProcessingStatus.acked,
    AckStatus.NACKED: ProcessingStatus.nacked,
    AckStatus.REJECTED: ProcessingStatus.rejected,
}
