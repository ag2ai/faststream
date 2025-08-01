import asyncio

from faststream import FastStream
from faststream.rabbit import RabbitBroker


async def main():
    broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
    app = FastStream(broker)
    await app.run()  # blocking method

if __name__ == "__main__":
    asyncio.run(main())