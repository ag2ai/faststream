import pytest
from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker, TestKafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )


@broker.subscriber("input_data")
async def handle(msg: Data, logger: Logger) -> None:
    logger.info(f"handle({msg=})")


@pytest.mark.asyncio
@pytest.mark.skip("waiting for feedback")
async def test_raw_publish():
    async with TestKafkaBroker(broker):
        msg = Data(data=0.5)

        await broker.publish(msg.model_dump_json().encode("utf-8"), "input_data")

        # handle.mock.assert_called_once_with(Data(data=0.5))
