import asyncio

from faststream import FastStream
from faststream.nats import NatsBroker


async def main():
    broker = NatsBroker("nats://localhost:4222")
    app = FastStream(broker)
    await app.run()  # blocking method

if __name__ == "__main__":
    asyncio.run(main())