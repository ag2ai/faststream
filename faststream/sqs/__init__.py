try:
    from faststream.sqs.annotations import SQSMessage
    from faststream.sqs.broker.broker import SQSBroker
    from faststream.sqs.broker.router import SQSPublisher, SQSRoute, SQSRouter
    from faststream.sqs.response import SQSResponse
    from faststream.sqs.schemas import (
        FifoQueue,
        RedriveAllowPolicy,
        RedrivePolicy,
        SQSQueue,
    )
    from faststream.sqs.testing import TestSQSBroker

except ImportError as e:
    if "aiobotocore" not in e.msg and "botocore" not in e.msg:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_SQS

    raise ImportError(INSTALL_FASTSTREAM_SQS) from e

__all__ = (
    "FifoQueue",
    "RedriveAllowPolicy",
    "RedrivePolicy",
    "SQSBroker",
    "SQSMessage",
    "SQSPublisher",
    "SQSQueue",
    "SQSResponse",
    "SQSRoute",
    "SQSRouter",
    "TestSQSBroker",
)
