from faststream import Context, FastStream, apply_types
from faststream.redis import RedisBroker
from faststream.redis.annotations import ContextRepo, RedisMessage

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber("test-channel")
async def handle(
    msg: str,
    message: RedisMessage,
    context: ContextRepo,
):
    with context.scope("correlation_id", message.correlation_id):
        call()


@apply_types(context__=app.context)
def call(
    message: RedisMessage,
    correlation_id=Context(),
):
    assert correlation_id == message.correlation_id
