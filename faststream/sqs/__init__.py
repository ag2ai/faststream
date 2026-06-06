from faststream._internal.parser import ParserProto
from faststream._internal.testing.app import TestApp

SQSParserType = ParserProto["MessageTypeDef"]  # type: ignore[name-defined]

try:
    from .annotations import SQSMessage
    from .broker.broker import SQSBroker
    from .broker.router import SQSPublisher, SQSRoute, SQSRouter
    from .response import SQSPublishCommand, SQSResponse
    from .schemas import (
        FifoQueue,
        RedriveAllowPolicy,
        RedrivePolicy,
        SQSQueue,
    )
    from .testing import TestSQSBroker

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
    "SQSParserType",
    "SQSPublishCommand",
    "SQSPublisher",
    "SQSQueue",
    "SQSResponse",
    "SQSRoute",
    "SQSRouter",
    "TestApp",
    "TestSQSBroker",
)
