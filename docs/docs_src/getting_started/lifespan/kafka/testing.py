import pytest

from faststream import FastStream, TestApp
from faststream.kafka import KafkaBroker, TestKafkaBroker

broker = KafkaBroker()
app = FastStream(broker)


@app.after_startup
async def handle():
    print("Calls in tests too!")


@pytest.mark.asyncio
async def test_lifespan():
    async with (
        TestKafkaBroker(broker, connect_only=True),
        TestApp(app),
    ):
        # test something
        pass
