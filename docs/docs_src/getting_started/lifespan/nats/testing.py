import pytest

from faststream import FastStream, TestApp
from faststream.nats import NatsBroker, TestNatsBroker

broker = NatsBroker()
app = FastStream(broker)


@app.after_startup
async def handle():
    print("Calls in tests too!")


@pytest.mark.asyncio
async def test_lifespan():
    async with (
        TestNatsBroker(broker, connect_only=True),
        TestApp(app),
    ):
        # test something
        pass
