from faststream import FastStream, Logger
from faststream.gcppubsub import GCPPubSubBroker

broker = GCPPubSubBroker(project_id="test-project")
app = FastStream(broker)


publisher = broker.publisher("response-topic")


@publisher
@broker.subscriber("test-subscription", topic="test-topic")
async def handle(msg: str, logger: Logger):
    logger.info(f"Received: {msg}")
    return f"Response: {msg}"


@app.after_startup
async def test_send():
    await broker.publish("Hello World!", "test-topic")
