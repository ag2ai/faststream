from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.specification.asyncapi import AsyncAPI

broker = KafkaBroker(
    "localhost:9092",
    description="Kafka broker running locally",
    specification_url="non-sensitive-url:9092",
)
app = FastStream(broker)
docs_obj = AsyncAPI(
    broker,
    schema_version="2.6.0",
)


@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg):
    # your processing logic
    pass
