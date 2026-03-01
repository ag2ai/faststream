import pytest

from faststream import FastStream, TestApp
from faststream.redis import RedisBroker, TestRedisBroker

app = FastStream(RedisBroker())


@app.after_startup
async def handle():
    print("Calls in tests too!")


@pytest.mark.asyncio
async def test_lifespan():
    async with (
        TestRedisBroker(app.broker, connect_only=True),
        TestApp(app),
    ):
        # test something
        pass
