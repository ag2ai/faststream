from faststream import Context, FastStream, apply_types, context
from faststream.redis import RedisBroker
from faststream.redis.annotations import RedisMessage

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber("test-channel")
async def handle(
    msg: str,
    message: RedisMessage,
):
    tag = context.set_local("correlation_id", message.correlation_id)
    call(tag)


@apply_types
def call(
    tag,
    message: RedisMessage,
    correlation_id: str = Context(),
):
    assert correlation_id == message.correlation_id
    context.reset_local("correlation_id", tag)
