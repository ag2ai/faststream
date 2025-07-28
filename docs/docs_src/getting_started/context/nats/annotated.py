from typing import Annotated
from faststream import Context, FastStream
from faststream.nats import NatsBroker
from faststream.nats.message import NatsMessage

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)

@broker.subscriber("test")
async def base_handler(
    body: str,
    message: Annotated[NatsMessage, Context()],  # get access to raw message
):
    ...
