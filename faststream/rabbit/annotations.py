from typing_extensions import Annotated

from faststream.annotations import ContextRepo, Logger, NoCast
from faststream.exceptions import INSTALL_FASTSTREAM_RABBIT
from faststream.rabbit.broker import RabbitBroker as RB
from faststream.rabbit.message import RabbitMessage as RM
from faststream.rabbit.publisher.producer import AioPikaFastProducer
from faststream.utils.context import Context

try:
    from aio_pika import RobustChannel, RobustConnection
except ImportError(INSTALL_FASTSTREAM_RABBIT) as e:
    raise ImportError(INSTALL_FASTSTREAM_RABBIT) from e

__all__ = (
    "Channel",
    "Connection",
    "ContextRepo",
    "Logger",
    "NoCast",
    "RabbitBroker",
    "RabbitMessage",
    "RabbitProducer",
)

RabbitMessage = Annotated[RM, Context("message")]
RabbitBroker = Annotated[RB, Context("broker")]
RabbitProducer = Annotated[AioPikaFastProducer, Context("broker._producer")]

Channel = Annotated[RobustChannel, Context("broker._channel")]
Connection = Annotated[RobustConnection, Context("broker._connection")]

# NOTE: transaction is not for the public usage yet
# async def _get_transaction(connection: Connection) -> RabbitTransaction:
#     async with connection.channel(publisher_confirms=False) as channel:
#         yield channel.transaction()

# Transaction = Annotated[RabbitTransaction, Depends(_get_transaction)]
