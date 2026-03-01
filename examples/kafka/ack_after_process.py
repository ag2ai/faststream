from faststream import FastStream, Logger, AckPolicy
from faststream.kafka import KafkaBroker

broker = KafkaBroker()
app = FastStream(broker)

@broker.subscriber(
    "test",
    group_id="group",
    ack_policy=AckPolicy.REJECT_ON_ERROR,
)
async def handler(msg: str, logger: Logger):
    logger.info(msg)


@app.after_startup
async def test():
    await broker.publish("Hi!", "test")
