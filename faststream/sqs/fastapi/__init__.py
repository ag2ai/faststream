from typing import Annotated

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.sqs.broker.broker import SQSBroker as SB  # noqa: N814
from faststream.sqs.message import SQSMessage as SM  # noqa: N814

from .fastapi import SQSRouter

__all__ = (
    "Context",
    "ContextRepo",
    "Logger",
    "SQSBroker",
    "SQSMessage",
    "SQSRouter",
)

SQSMessage = Annotated[SM, Context("message")]
SQSBroker = Annotated[SB, Context("broker")]
