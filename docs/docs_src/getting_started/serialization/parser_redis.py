from typing import Awaitable, Callable

from faststream import FastStream
from faststream.redis import RedisBroker, RedisMessage
from faststream.redis.message import PubSubMessage


async def custom_parser(
    msg: PubSubMessage,
    original_parser: Callable[[PubSubMessage], Awaitable[RedisMessage]],
) -> RedisMessage:
    parsed_msg = await original_parser(msg)
    parsed_msg.message_id = parsed_msg.headers.get("custom_message_id")
    return parsed_msg


broker = RedisBroker(parser=custom_parser)
app = FastStream(broker)


@broker.subscriber("test")
async def handle():
    ...


@app.after_startup
async def test():
    await broker.publish("", "test", headers={"custom_message_id": "1"})
