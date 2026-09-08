from faststream import FastStream, Logger
from faststream.confluent import KafkaBroker, Topic

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber(
    Topic("orders", num_partitions=3, replication_factor=2),
    Topic("legacy-orders", declare=False),
    "audit",
)
async def on_order(msg: str, logger: Logger):
    logger.info(msg)
