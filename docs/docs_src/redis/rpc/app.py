from faststream import FastStream, Logger
from faststream.redis import ListSub, PubSub, RedisBroker, RedisChannelMessage, StreamSub

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)
channel = PubSub("test-channel")
list_sub = ListSub("test-list")
stream = StreamSub("test-stream")


@broker.subscriber(channel=channel)
async def handle_channel(msg: str, logger: Logger) -> str:
    logger.info(msg)
    return msg


@broker.subscriber(list=list_sub)
async def handle_list(msg: str, logger: Logger) -> str:
    logger.info(msg)
    return msg


@broker.subscriber(stream=stream)
async def handle_stream(msg: str, logger: Logger) -> str:
    logger.info(msg)
    return msg


@app.after_startup
async def t() -> None:
    msg = "Hi!"

    response: RedisChannelMessage = await broker.request(
        "Hi!",
        channel=channel,
        timeout=3.0,
    )
    assert await response.decode() == msg

    response = await broker.request(
        "Hi!",
        list=list_sub,
        timeout=3.0,
    )
    assert await response.decode() == msg

    response = await broker.request(
        "Hi!",
        stream=stream,
        timeout=3.0,
    )
    assert await response.decode() == msg
