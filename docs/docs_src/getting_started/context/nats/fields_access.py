from typing import Annotated
from faststream import Context, FastStream
from faststream.nats import NatsBroker, NatsMessage

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)

@broker.subscriber("test-subject")
async def handle(
    msg: NatsMessage,
    correlation_id: Annotated[str, Context("message.correlation_id")],
    user_header: Annotated[str, Context("message.headers.user")],
):
    assert msg.correlation_id == correlation_id
    assert msg.headers["user"] == user_header
