from faststream import FastStream, Logger
from faststream.nats import JStream, NatsBroker
from nats.js.api import DeliverPolicy

broker = NatsBroker()
app = FastStream(broker)

stream = JStream(name="stream")

@broker.subscriber(
    "js-subject",
    stream=stream,
    deliver_policy=DeliverPolicy.NEW,
)
async def handler(msg: str, logger: Logger):
    logger.info(msg)

@app.after_startup
async def test_send():
    await broker.publish("Hi!", "js-subject")
    # publish with stream verification
    await broker.publish("Hi!", "js-subject", stream="stream")
