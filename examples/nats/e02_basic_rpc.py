from faststream import FastStream, Logger
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream(broker)


@broker.subscriber("subject")
async def handler(msg: str, logger: Logger):
    logger.info(msg)
    return "Response"


@app.after_startup
async def test_send():
    response = await broker.request("Hi!", "subject")
    assert await response.decode() == "Response"
