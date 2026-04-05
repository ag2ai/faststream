from faststream import AckPolicy, FastStream, Logger
from faststream.kafka import KafkaBroker

broker = KafkaBroker()
app = FastStream(broker)


@broker.subscriber(
    "test",
    group_id="group",
    ack_policy=AckPolicy.ACK,
)
async def handler(msg: str, logger: Logger) -> None:
    logger.info(msg)


@app.after_startup
async def test() -> None:
    await broker.publish("Hi!", "test")
