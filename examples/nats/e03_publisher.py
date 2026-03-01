from faststream import FastStream, Logger
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream(broker)


@broker.subscriber("subject")
@broker.publisher("response-subject")
async def handler(msg: str, logger: Logger):
    logger.info(msg)
    return "Response"


@broker.subscriber("response-subject")
async def response_handler(msg: str, logger: Logger):
    logger.info(msg)
    assert msg == "Response"


@app.after_startup
async def test_send():
    await broker.publish("Hi!", "subject")
