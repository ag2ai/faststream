import os
from faststream import FastStream, Logger
from faststream.gcp import GCPBroker

# Use environment variables for configuration
broker = GCPBroker(
    project_id=os.getenv("GCP_PROJECT_ID", "test-project"),
    emulator_host=os.getenv("PUBSUB_EMULATOR_HOST"),
)
app = FastStream(broker)


@broker.subscriber("test-subscription", topic="test-topic")
async def handle(msg: str, logger: Logger):
    logger.info(msg)


@app.after_startup
async def test_send():
    await broker.publish("Hi!", "test-topic")
