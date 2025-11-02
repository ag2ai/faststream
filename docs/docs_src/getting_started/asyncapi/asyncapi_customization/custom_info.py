from typing import Any
from faststream import FastStream
from faststream.specification import AsyncAPI, License, Contact
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")

app = FastStream(
    broker,
    specification=AsyncAPI(
        title="My App",
        version="1.0.0",
        description=(
            "# Title of the description\n"
            "This description supports **Markdown** syntax"
        ),
        license=License(name="MIT", url="https://opensource.org/license/mit/"),
        terms_of_service="https://my-terms.com/",
        contact=Contact(name="support", url="https://help.com/"),
        schema_version="2.6.0",  # you can choose AsyncAPI version
    ),
)

@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg: Any) -> None:
    # your processing logic
    pass
