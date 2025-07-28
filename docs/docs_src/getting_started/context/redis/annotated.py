from typing import Annotated
from faststream import Context, FastStream
from faststream.redis import RedisBroker
from faststream.redis.message import RedisMessage

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)

@broker.subscriber("test")
async def base_handler(
    body: str,
    message: Annotated[RedisMessage, Context()],  # get access to raw message
):
    ...
