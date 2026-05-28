from faststream import Context, FastStream
from faststream.mqtt import MQTTBroker
from faststream.mqtt.annotations import (
    ContextRepo,
    MQTTMessage,
    Logger,
    MQTTBroker as BrokerAnnotation,
)

broker_object = MQTTBroker("localhost", port=1883)
app = FastStream(broker_object)

@broker_object.subscriber("test-topic")
async def handle(
    logger=Context(),
    message=Context(),
    broker=Context(),
    context=Context(),
):
    logger.info(message)
    await broker.publish("test", "response")

@broker_object.subscriber("response-topic")
async def handle_response(
    logger: Logger,
    message: MQTTMessage,
    context: ContextRepo,
    broker: BrokerAnnotation,
):
    logger.info(message)
    await broker.publish("test", "response")
