from typing import Annotated

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast
from faststream.sqs.broker.broker import SQSBroker as SB  # noqa: N814
from faststream.sqs.message import SQSMessage as SM  # noqa: N814

__all__ = (
    "ContextRepo",
    "Logger",
    "NoCast",
    "SQSBroker",
    "SQSMessage",
)

SQSMessage = Annotated[SM, Context("message")]
SQSBroker = Annotated[SB, Context("broker")]
