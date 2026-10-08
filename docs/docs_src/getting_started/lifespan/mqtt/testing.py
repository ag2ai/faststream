import pytest

from faststream import FastStream, TestApp
from faststream.mqtt import MQTTBroker, TestMQTTBroker

broker = MQTTBroker()
app = FastStream(broker)


@app.after_startup
async def handle():
    print("Calls in tests too!")


@pytest.mark.asyncio
async def test_lifespan():
    async with (
        TestMQTTBroker(broker, connect_only=True),
        TestApp(app),
    ):
        # test something
        pass
