from typing import Awaitable, Callable

from aio_pika import IncomingMessage

from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage


async def custom_parser(
    msg: IncomingMessage,
    original_parser: Callable[[IncomingMessage], Awaitable[RabbitMessage]],
) -> RabbitMessage:
    parsed_msg = await original_parser(msg)
    parsed_msg.message_id = parsed_msg.headers.get("custom_message_id")
    return parsed_msg


broker = RabbitBroker(parser=custom_parser)
app = FastStream(broker)


@broker.subscriber("test")
async def handle():
    ...


@app.after_startup
async def test():
    await broker.publish("", "test", headers={"custom_message_id": "1"})
