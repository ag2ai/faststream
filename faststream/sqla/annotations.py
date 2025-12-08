from typing import Annotated

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast
from faststream.sqla.message import SqlaMessage as SM
from faststream.sqla.broker.broker import SqlaBroker as SB

__all__ = (
    "SqlaMessage",
    "SqlaBroker",
)

SqlaMessage = Annotated[SM, Context("message")]
SqlaBroker = Annotated[SB, Context("broker")]