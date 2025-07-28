from typing import Annotated
from faststream import Context, FastStream
from faststream.confluent import KafkaBroker
from faststream.confluent.message import KafkaMessage

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@broker.subscriber("test")
async def base_handler(
    body: str,
    message: Annotated[KafkaMessage, Context()],  # get access to raw message
):
    ...
