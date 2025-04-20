import pytest
from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream, Logger
from faststream._internal._compat import model_to_json
from faststream.confluent import KafkaBroker, TestKafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


class Data(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )

prepared_publisher = broker.publisher("input_data")

@broker.subscriber("input_data")
async def handle_data(msg: Data, logger: Logger) -> None:
    logger.info("handle_data(msg=%s)", msg)

@pytest.mark.asyncio
async def test_prepared_publish():
    async with TestKafkaBroker(broker):
        msg = Data(data=0.5)

        await prepared_publisher.publish(
            model_to_json(msg),
            headers={"content-type": "application/json"},
        )

        handle_data.mock.assert_called_once_with(dict(msg))
