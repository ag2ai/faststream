from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("topic1", "topic2", "topic3")
async def on_multiple_topics(msg: str, logger: Logger):
    logger.info(msg)
