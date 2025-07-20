from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.specification import AsyncAPI

broker = KafkaBroker("localhost:9092")
app = FastStream(broker, specification=AsyncAPI(schema_version="2.6.0"))
asyncapi = app.schema.to_specification()

@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg):
    # your processing logic
    pass
