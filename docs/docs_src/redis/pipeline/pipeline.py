from faststream import FastStream, Logger
from faststream.redis import RedisBroker, Pipeline

broker = RedisBroker()
app = FastStream(broker)

@broker.subscriber("test")
async def handle(
    msg: str,
    logger: Logger,
    pipe: Pipeline,
) -> None:
    logger.info(msg)

    for i in range(10):
        await broker.publish(
            f"hello {i}",
            channel="test-output",  # queue can be channel, list, or stream
            pipeline=pipe,
        )

    results = await pipe.execute()  # execute all publish commands
    logger.info(results)

@app.after_startup
async def t() -> None:
    await broker.publish("Hi!", "test")