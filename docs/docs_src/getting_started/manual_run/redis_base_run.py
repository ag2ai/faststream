import asyncio

from faststream import FastStream
from faststream.redis import RedisBroker


async def main():
    broker = RedisBroker("redis://localhost:6379")
    app = FastStream(broker)
    await app.run()  # blocking method

if __name__ == "__main__":
    asyncio.run(main())