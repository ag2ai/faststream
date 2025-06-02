from faststream import Context, FastStream
from faststream.nats import NatsBroker, NatsMessage

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@broker.subscriber("test")
async def base_handler(
    body: str,
    message: NatsMessage = Context(),  # get access to raw message
):
    ...
