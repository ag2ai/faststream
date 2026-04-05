import zmqtt

from faststream.message import StreamMessage


class MQTTMessage(StreamMessage[zmqtt.Message]):
    """A class to represent an MQTT message."""

    async def ack(self) -> None:
        if self.committed is None and self.raw_message.qos != zmqtt.QoS.AT_MOST_ONCE:
            await self.raw_message.ack()
        await super().ack()

    async def nack(self) -> None:
        await self.ack()

    async def reject(self) -> None:
        await self.ack()
