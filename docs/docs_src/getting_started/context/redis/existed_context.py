from faststream import Context, FastStream, Logger, ContextRepo
from faststream.redis import RedisBroker, RedisMessage
from faststream.redis.annotations import (
    ContextRepo,
    RedisMessage,
    Logger,
    RedisBroker as BrokerAnnotation,
)

broker_object = RedisBroker("redis://localhost:6379")
app = FastStream(broker_object)


@broker_object.subscriber("test-channel")
async def handle(
    msg: str,
    logger: Logger = Context(),
    message: RedisMessage = Context(),
    broker: RedisBroker = Context(),
    context: ContextRepo = Context(),
):
    logger.info(message)
    await broker.publish("test", "response")


@broker_object.subscriber("response-channel")
async def handle_response(
    msg: str,
    logger: Logger,
    message: RedisMessage,
    context: ContextRepo,
    broker: BrokerAnnotation,
):
    logger.info(message)
    await broker.publish("test", "response")
