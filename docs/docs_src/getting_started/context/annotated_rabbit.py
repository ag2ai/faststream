from typing import Annotated

from faststream import Context, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage

Message = Annotated[RabbitMessage, Context()]

broker = RabbitBroker()
app = FastStream(broker)


@broker.subscriber("test")
async def base_handler(
    body: str,
    message: Message,  # get access to raw message
):
    ...
