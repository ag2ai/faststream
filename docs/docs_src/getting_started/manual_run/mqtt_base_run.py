import asyncio

from faststream import FastStream
from faststream.mqtt import MQTTBroker


async def main():
    broker = MQTTBroker("localhost", port=1883)
    app = FastStream(broker)
    await app.run()  # blocking method

if __name__ == "__main__":
    asyncio.run(main())
