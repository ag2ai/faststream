from faststream import Context, FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import (
    ContextRepo,
    RabbitMessage,
    Logger,
    RabbitBroker as BrokerAnnotation,
)

broker_object = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker_object)


@broker_object.subscriber("test-queue")
async def handle(
    msg: str,
    logger=Context(),
    message=Context(),
    broker=Context(),
    context=Context(),
):
    logger.info(message)
    await broker.publish("test", "response")


@broker_object.subscriber("response-queue")
async def handle_response(
    msg: str,
    logger: Logger,
    message: RabbitMessage,
    context: ContextRepo,
    broker: BrokerAnnotation,
):
    logger.info(message)
    await broker.publish("test", "response")
