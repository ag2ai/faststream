from .channel import Channel
from .constants import ExchangeType
from .exchange import RabbitExchange
from .proto import BaseRMQInformation
from .queue import QueueType, RabbitQueue
from .reply import ReplyConfig

__all__ = (
    "RABBIT_REPLY",
    "BaseRMQInformation",
    "Channel",
    "ExchangeType",
    "QueueType",
    "RabbitExchange",
    "RabbitQueue",
    "ReplyConfig",
)

RABBIT_REPLY = RabbitQueue("amq.rabbitmq.reply-to", declare=False)
