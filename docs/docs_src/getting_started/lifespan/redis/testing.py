import pytest

from faststream import FastStream, TestApp
from faststream.redis import RedisBroker, TestRedisBroker

broker = RedisBroker()
app = FastStream(broker)


@app.after_startup
async def handle():
    print("Calls in tests too!")


@pytest.mark.asyncio
async def test_lifespan():
    async with (
        TestRedisBroker(broker, connect_only=True),
        TestApp(app),
    ):
        # test something
        pass
