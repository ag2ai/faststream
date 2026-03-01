import pytest

from faststream import FastStream, TestApp
from faststream.rabbit import RabbitBroker, TestRabbitBroker

app = FastStream(RabbitBroker())


@app.after_startup
async def handle():
    print("Calls in tests too!")


@pytest.mark.asyncio
async def test_lifespan():
    async with (
        TestRabbitBroker(app.broker, connect_only=True),
        TestApp(app),
    ):
        # test something
        pass
