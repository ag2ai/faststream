from .channel import Channel
from .constants import ExchangeType
from .exchange import RabbitExchange
from .queue import QueueType, RabbitQueue

__all__ = (
    "RABBIT_REPLY",
    "Channel",
    "ExchangeType",
    "QueueType",
    "RabbitExchange",
    "RabbitQueue",
)

RABBIT_REPLY = RabbitQueue("amq.rabbitmq.reply-to", declare=False)
