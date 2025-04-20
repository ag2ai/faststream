from dataclasses import dataclass
from typing import Optional


@dataclass
class Channel:
    """Channel class that represents a RabbitMQ channel."""

    prefetch_count: Optional[int] = None

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
