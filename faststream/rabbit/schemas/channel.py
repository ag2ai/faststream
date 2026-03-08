from dataclasses import dataclass


@dataclass(eq=False)
class Channel:
    """Channel class that represents a RabbitMQ channel."""

    prefetch_count: int | None = None
    """Limit the number of unacknowledged messages on a channel
    https://www.rabbitmq.com/docs/consumer-prefetch
    """

    global_qos: bool = False
    """Share the limit between all channel' subscribers
    https://www.rabbitmq.com/docs/consumer-prefetch#sharing-the-limit
    """

    channel_number: int | None = None
    """Specify the channel number explicit."""

    publisher_confirms: bool = True
    """if `True` the :func:`aio_pika.Exchange.publish` method will be
    return :class:`bool` after publish is complete. Otherwise the
    :func:`aio_pika.Exchange.publish` method will be return
    :class:`None`"""

    on_return_raises: bool = False
    """raise an :class:`aio_pika.exceptions.DeliveryError`
    when mandatory message will be returned"""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Channel):
            return NotImplemented
        return (
            self.prefetch_count == other.prefetch_count
            and self.global_qos == other.global_qos
            and self.channel_number == other.channel_number
            and self.publisher_confirms == other.publisher_confirms
            and self.on_return_raises == other.on_return_raises
        )

    def __hash__(self) -> int:
        return hash((
            self.prefetch_count,
            self.global_qos,
            self.channel_number,
            self.publisher_confirms,
            self.on_return_raises,
        ))
