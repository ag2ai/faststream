from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker


def subscriber_exception_handler(exc: BaseException) -> bool:
    return isinstance(exc, ValueError)


async def broker_exception_handler(exc: BaseException) -> bool:
    return isinstance(exc, RuntimeError)


async def app_exception_handler(
    exc: BaseException,
    logger: Logger,
) -> bool:
    logger.error("Unhandled message processing error", exc_info=exc)
    return False


broker = KafkaBroker(
    "localhost:9092",
    exception_handler=broker_exception_handler,
)
app = FastStream(broker, exception_handler=app_exception_handler)


@broker.subscriber(
    "exception-handlers",
    exception_handler=subscriber_exception_handler,
)
async def handle(message: str) -> None:
    if message == "subscriber":
        raise ValueError("subscriber error")
    if message == "broker":
        raise RuntimeError("broker error")
    raise TypeError("application error")
