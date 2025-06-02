from faststream import Context, FastStream, Logger, ContextRepo
from faststream.kafka import KafkaBroker, KafkaMessage
from faststream.kafka.annotations import (
    ContextRepo,
    KafkaMessage,
    Logger,
    KafkaBroker as BrokerAnnotation,
)

broker_object = KafkaBroker("localhost:9092")
app = FastStream(broker_object)


@broker_object.subscriber("test-topic")
async def handle(
    msg: str,
    logger: Logger = Context(),
    message: KafkaMessage = Context(),
    broker: KafkaBroker = Context(),
    context: ContextRepo = Context(),
):
    logger.info(message)
    await broker.publish("test", "response")


@broker_object.subscriber("response-topic")
async def handle_response(
    msg: str,
    logger: Logger,
    message: KafkaMessage,
    context: ContextRepo,
    broker: BrokerAnnotation,
):
    logger.info(message)
    await broker.publish("test", "response")
