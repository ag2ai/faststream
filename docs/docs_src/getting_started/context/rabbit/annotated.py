from typing import Annotated
from faststream import Context, FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.message import RabbitMessage

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

@broker.subscriber("test")
async def base_handler(
    body: str,
    message: Annotated[RabbitMessage, Context()],  # get access to raw message
):
    ...
