from faststream import Context, FastStream
from faststream.redis import RedisBroker
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
    logger=Context(),
    message=Context(),
    broker=Context(),
    context=Context(),
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
