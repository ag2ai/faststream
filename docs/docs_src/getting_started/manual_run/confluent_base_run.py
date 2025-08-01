import asyncio

from faststream import FastStream
from faststream.confluent import KafkaBroker


async def main():
    broker = KafkaBroker("localhost:9092")
    app = FastStream(broker)
    await app.run()  # blocking method

if __name__ == "__main__":
    asyncio.run(main())
