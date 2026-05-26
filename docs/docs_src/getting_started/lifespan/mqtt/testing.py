import pytest

from faststream import FastStream, TestApp
from faststream.mqtt import MQTTBroker, TestMQTTBroker

app = FastStream(MQTTBroker())


@app.after_startup
async def handle():
    print("Calls in tests too!")


@pytest.mark.asyncio
async def test_lifespan():
    async with (
        TestMQTTBroker(app.broker, connect_only=True),
        TestApp(app),
    ):
        # test something
        pass
