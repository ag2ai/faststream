import pytest
from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream, Logger
from faststream.redis import RedisBroker, TestRedisBroker


class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )


broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber("input_data")
async def on_input_data(msg: Data, logger: Logger):
    logger.info("on_input_data(msg=%s)", msg)


@pytest.mark.asyncio
async def test_raw_publish():
    async with TestRedisBroker(broker):
        msg = Data(data=0.5)

        await broker.publish(msg, "input_data")

        on_input_data.mock.assert_called_once_with(dict(msg))
