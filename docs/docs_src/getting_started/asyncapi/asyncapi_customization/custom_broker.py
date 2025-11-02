from typing import Any
from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.specification import AsyncAPI

broker = KafkaBroker(
    "localhost:9092",
    description="Kafka broker running locally",
    specification_url="non-sensitive-url:9092",  # overrides original one
)

app = FastStream(broker, specification=AsyncAPI())

@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg: Any) -> None:
    # your processing logic
    pass
