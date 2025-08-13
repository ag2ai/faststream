from faststream import FastStream, Logger
from faststream.gcp import GCPBroker, Pipeline

broker = GCPBroker(project_id="test-project-id")
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
            channel="test-output",  # destination can be channel, list, or stream
            pipeline=pipe,
        )

    results = await pipe.execute()  # execute all publish commands
    logger.info(results)

@app.after_startup
async def t() -> None:
    await broker.publish("Hi!", "test")
