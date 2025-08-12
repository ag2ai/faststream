from faststream import FastStream, Logger
from faststream.gcppubsub import GCPPubSubBroker

broker = GCPPubSubBroker(project_id="test-project")
app = FastStream(broker)


@broker.subscriber("test-subscription", topic="test-topic")
async def handle(msg: str, logger: Logger):
    logger.info(msg)


@app.after_startup
async def test_send():
    await broker.publish("Hi!", "test-topic")