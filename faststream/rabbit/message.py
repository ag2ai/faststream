from aio_pika import IncomingMessage

from faststream.message import StreamMessage


class RabbitMessage(StreamMessage[IncomingMessage]):
    """A message class for working with RabbitMQ messages.

    This class extends `StreamMessage` to provide additional functionality for acknowledging, rejecting,
    or nack-ing RabbitMQ messages.
    """

    async def ack(
        self,
        multiple: bool = False,
    ) -> None:
        """Acknowledge the RabbitMQ message."""
        pika_message = self.raw_message
        await super().ack()
        if pika_message.locked:
            return
        await pika_message.ack(multiple=multiple)

    async def nack(
        self,
        multiple: bool = False,
        requeue: bool = True,
    ) -> None:
        """Negative Acknowledgment of the RabbitMQ message."""
        pika_message = self.raw_message
        await super().nack()
        if pika_message.locked:
            return
        await pika_message.nack(multiple=multiple, requeue=requeue)

    async def reject(
        self,
        requeue: bool = False,
    ) -> None:
        """Reject the RabbitMQ message."""
        pika_message = self.raw_message
        await super().reject()
        if pika_message.locked:
            return
        await pika_message.reject(requeue=requeue)
