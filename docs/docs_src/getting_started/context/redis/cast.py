from typing import Annotated
from faststream import Context, FastStream
from faststream.context import ContextRepo
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker, context=ContextRepo({
    "secret": "1"
}))

@broker.subscriber("test-channel")
async def handle(
    secret: Annotated[int, Context()],
):
    assert secret == "1"

@broker.subscriber("test-channel2")
async def handle_int(
    secret: Annotated[int, Context(cast=True)],
):
    assert secret == 1
