from faststream import FastStream
from faststream.annotations import Logger
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue", filter=lambda m: m.content_type == "application/json")
async def handle_json(msg, logger: Logger):
    logger.info("JSON message: %s", msg)


@broker.subscriber("test-queue")
async def handle_other_messages(msg, logger: Logger):
    logger.info("Default message: %s", msg)


@app.after_startup
async def test_publishing():
    # send to `handle_json`
    await broker.publish({"msg": "Hello!"}, "test-queue")

    # send to `handle_other_messages`
    await broker.publish("Hello!", "test-queue")
