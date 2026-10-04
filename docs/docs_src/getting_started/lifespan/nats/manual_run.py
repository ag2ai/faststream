import asyncio
import signal

from faststream import FastStream, Logger
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream(broker)


@broker.subscriber("test-subject")
async def handle(msg: str, logger: Logger) -> None:
    logger.info(msg)


async def serve(stop: asyncio.Event) -> None:
    await app.start()
    try:
        await stop.wait()
    finally:
        await app.stop()


async def main() -> None:
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await serve(stop)


if __name__ == "__main__":
    asyncio.run(main())
