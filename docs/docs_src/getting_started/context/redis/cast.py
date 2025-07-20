from faststream import Context, FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)
app.context.set_global("secret", "1")

@broker.subscriber("test-channel")
async def handle(
    secret: int = Context(),
):
    assert secret == "1"

@broker.subscriber("test-channel2")
async def handle_int(
    secret: int = Context(cast=True),
):
    assert secret == 1
