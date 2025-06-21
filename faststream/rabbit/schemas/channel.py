from dataclasses import dataclass
from typing import Optional


@dataclass
class Channel:
    """Channel class that represents a RabbitMQ channel."""

    prefetch_count: Optional[int] = None
    """Limit the number of unacknowledged messages on a channel
    https://www.rabbitmq.com/docs/consumer-prefetch
    """

    global_qos: bool = False
    """Share the limit between all channel' subscribers
    https://www.rabbitmq.com/docs/consumer-prefetch#sharing-the-limit
    """

    channel_number: Optional[int] = None
    """Specify the channel number explicit."""

    publisher_confirms: bool = True
    """if `True` the :func:`aio_pika.Exchange.publish` method will be
    return :class:`bool` after publish is complete. Otherwise the
    :func:`aio_pika.Exchange.publish` method will be return
    :class:`None`"""

    on_return_raises: bool = False
    """raise an :class:`aio_pika.exceptions.DeliveryError`
    when mandatory message will be returned"""

    def __hash__(self) -> int:
        return id(self)
