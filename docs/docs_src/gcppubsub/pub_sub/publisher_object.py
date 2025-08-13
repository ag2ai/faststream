import pytest
from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream, Logger
from faststream.gcp import GCPBroker, TestGCPBroker

broker = GCPBroker(project_id="test-project-id")
app = FastStream(broker)


class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example",
    )

prepared_publisher = broker.publisher("input_data")

@broker.subscriber("input_data")
async def handle_data(msg: Data, logger: Logger) -> None:
    logger.info("handle_data(msg=%s)", msg)

@pytest.mark.asyncio
async def test_prepared_publish():
    async with TestGCPBroker(broker):
        msg = Data(data=0.5)

        await prepared_publisher.publish(msg)

        handle_data.mock.assert_called_once_with(dict(msg))
