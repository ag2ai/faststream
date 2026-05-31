from typing import Awaitable, Callable

from faststream import FastStream
from faststream.mqtt import MQTTBroker, MQTTMessage
from zmqtt import Message as RawMessage


async def custom_parser(
    msg: RawMessage,
    original_parser: Callable[[RawMessage], Awaitable[MQTTMessage]],
) -> MQTTMessage:
    parsed_msg = await original_parser(msg)
    parsed_msg.message_id = parsed_msg.headers.get("custom_message_id")
    return parsed_msg


broker = MQTTBroker(parser=custom_parser)
app = FastStream(broker)


@broker.subscriber("test")
async def handle():
    ...


@app.after_startup
async def test():
    await broker.publish("", "test", headers={"custom_message_id": "1"})
