from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker, KafkaRouter

router = KafkaRouter("prefix_")


@router.subscriber("in")
@router.publisher("out")
async def handle(msg: str, logger: Logger):
    logger.info(msg)
    return "Response"


@router.subscriber("out")
async def handle_response(msg: str, logger: Logger):
    logger.info(msg)


broker = KafkaBroker("localhost:9092")
broker.include_router(router)

app = FastStream(broker)


@app.after_startup
async def test() -> None:
    await broker.publish("Hello!", "prefix_in")
