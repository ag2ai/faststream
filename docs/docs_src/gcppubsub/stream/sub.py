from faststream import FastStream, Logger
from faststream.gcp import GCPBroker

broker = GCPBroker(project_id="test-project-id")
app = FastStream(broker)


@broker.subscriber(stream="test-stream")
async def handle(msg: str, logger: Logger):
    logger.info(msg)
