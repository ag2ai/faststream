from faststream import Context, FastStream
from faststream.nats import NatsBroker
from faststream.nats.annotations import (
    ContextRepo,
    NatsMessage,
    Logger,
    NatsBroker as BrokerAnnotation,
)

broker_object = NatsBroker("nats://localhost:4222")
app = FastStream(broker_object)


@broker_object.subscriber("test-subject")
async def handle(
    msg: str,
    logger=Context(),
    message=Context(),
    broker=Context(),
    context=Context(),
):
    logger.info(message)
    await broker.publish("test", "response")


@broker_object.subscriber("response-subject")
async def handle_response(
    msg: str,
    logger: Logger,
    message: NatsMessage,
    context: ContextRepo,
    broker: BrokerAnnotation,
):
    logger.info(message)
    await broker.publish("test", "response")
