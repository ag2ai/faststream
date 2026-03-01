from faststream import FastStream, Logger
from faststream.nats import NatsBroker, PullSub

broker = NatsBroker()
app = FastStream(broker)


@broker.subscriber(
    subject="test",
    stream="stream",
    pull_sub=PullSub(batch_size=10),
)
async def handle(msg, logger: Logger):
    logger.info(msg)
