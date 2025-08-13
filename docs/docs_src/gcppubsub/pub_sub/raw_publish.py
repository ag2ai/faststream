import pytest
from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream, Logger
from faststream.gcp import GCPBroker, TestGCPBroker


class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example",
    )


broker = GCPBroker(project_id="test-project-id")
app = FastStream(broker)


@broker.subscriber("input_data")
async def on_input_data(msg: Data, logger: Logger):
    logger.info("on_input_data(msg=%s)", msg)


@pytest.mark.asyncio
async def test_raw_publish():
    async with TestGCPBroker(broker):
        msg = Data(data=0.5)

        await broker.publish(msg, "input_data")

        on_input_data.mock.assert_called_once_with(dict(msg))
