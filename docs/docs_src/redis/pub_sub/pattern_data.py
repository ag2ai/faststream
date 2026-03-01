from faststream import FastStream, Logger, Path
from faststream.redis import RedisBroker

broker = RedisBroker()
app = FastStream(broker)


@broker.subscriber("test.{data}")
async def handle_test(
    msg: str,
    logger: Logger,
    data: str = Path(),
):
    logger.info("Channel `data=%s`, body `msg=%s`", data, msg)
