import pytest

from faststream import FastStream, TestApp
from faststream.confluent import KafkaBroker, TestKafkaBroker

app = FastStream(KafkaBroker())


@app.after_startup
async def handle():
    print("Calls in tests too!")


@pytest.mark.asyncio
async def test_lifespan():
    async with (
        TestKafkaBroker(app.broker, connect_only=True),
        TestApp(app),
    ):
        # test something
        pass
