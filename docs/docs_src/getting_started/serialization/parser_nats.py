from typing import Awaitable, Callable

from nats.aio.msg import Msg

from faststream import FastStream
from faststream.nats import NatsBroker, NatsMessage


async def custom_parser(
    msg: Msg,
    original_parser: Callable[[Msg], Awaitable[NatsMessage]],
) -> NatsMessage:
    parsed_msg = await original_parser(msg)
    parsed_msg.message_id = parsed_msg.headers.get("custom_message_id")
    return parsed_msg


broker = NatsBroker(parser=custom_parser)
app = FastStream(broker)


@broker.subscriber("test")
async def handle():
    ...


@app.after_startup
async def test():
    await broker.publish("", "test", headers={"custom_message_id": "1"})
