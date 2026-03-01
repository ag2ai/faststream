from faststream import FastStream, Logger
from faststream.redis import RedisBroker

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber(channel="test-channel")
async def handle_channel(msg: str, logger: Logger):
    logger.info(msg)
    return msg


@broker.subscriber(list="test-list")
async def handle_list(msg: str, logger: Logger):
    logger.info(msg)
    return msg


@broker.subscriber(stream="test-stream")
async def handle_stream(msg: str, logger: Logger):
    logger.info(msg)
    return msg


@app.after_startup
async def t():
    msg = "Hi!"

    response = await broker.request(
        "Hi!",
        channel="test-channel",
        timeout=3.0,
    )
    assert await response.decode() == msg

    response = await broker.request(
        "Hi!",
        list="test-list",
        timeout=3.0,
    )
    assert await response.decode() == msg

    response = await broker.request(
        "Hi!",
        stream="test-stream",
        timeout=3.0,
    )
    assert await response.decode() == msg
