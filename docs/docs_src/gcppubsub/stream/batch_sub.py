from faststream import FastStream, Logger
from faststream.gcp import GCPBroker, StreamSub

broker = GCPBroker(project_id="test-project-id")
app = FastStream(broker)


@broker.subscriber(stream=StreamSub("test-stream", batch=True))
async def handle(msg: list[str], logger: Logger):
    logger.info(msg)
