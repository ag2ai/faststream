from typing import Annotated

from faststream import Context, FastStream, Logger
from faststream.nats import NatsBroker, PullSub, message

broker = NatsBroker()

@broker.subscriber(
    "test",
    stream="test",
    durable="test",
    pull_sub=PullSub(batch=True, batch_size=3),
)
async def handler(
    bodies: list[str],
    logger: Logger,
    msg: Annotated[message.NatsBatchMessage, Context("message")],
) -> None:
    logger.info(bodies)
    for m in msg.raw_message:
        await m.ack()

app = FastStream(broker)

@app.after_startup
async def after_startup() -> None:
    for i in range(3):
        await broker.publish(f"Hello, world! {i}", "test", stream="test")
