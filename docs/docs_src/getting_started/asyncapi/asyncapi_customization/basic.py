from typing import Any
from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.specification import AsyncAPI

broker = KafkaBroker("localhost:9092")
app = FastStream(broker, specification=AsyncAPI())
asyncapi = app.schema.to_specification()

@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg: Any) -> None:
    # your processing logic
    pass
