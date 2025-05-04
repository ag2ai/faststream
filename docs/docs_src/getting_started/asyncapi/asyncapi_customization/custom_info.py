from faststream import FastStream
from faststream.specification.asyncapi import AsyncAPI
from faststream.specification import License, Contact
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
description="""# Title of the description
This description supports **Markdown** syntax"""
app = FastStream(broker)
docs_obj = AsyncAPI(
    broker,
    title="My App",
    app_version="1.0.0",
    description=description,
    license=License(name="MIT", url="https://opensource.org/license/mit/"),
    terms_of_service="https://my-terms.com/",
    contact=Contact(name="support", url="https://help.com/"),
    schema_version="2.6.0",
)

@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg):
    # your processing logic
    pass
