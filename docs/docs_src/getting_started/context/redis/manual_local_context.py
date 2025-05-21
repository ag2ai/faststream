from faststream import Context, FastStream, apply_types
from faststream.redis import RedisBroker
from faststream.redis.annotations import RedisMessage

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber("test-channel")
async def handle(
    msg: str,
    message: RedisMessage,
):
    tag = app.context.set_local("correlation_id", message.correlation_id)
    call(tag)


@apply_types(context__=app.context)
def call(
    tag,
    message: RedisMessage,
    correlation_id=Context(),
):
    assert correlation_id == message.correlation_id
    app.context.reset_local("correlation_id", tag)
