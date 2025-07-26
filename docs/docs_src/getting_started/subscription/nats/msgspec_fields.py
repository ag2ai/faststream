from msgspec import field
from fast_depends.msgspec import MsgSpecSerializer

from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker(
    "nats://localhost:4222",
    serializer=MsgSpecSerializer(),
)
app = FastStream(broker)


@broker.subscriber("test-subject")
async def handle(
    name: str,
    user_id: int = field(name="userId"),
):
    assert name == "John"
    assert user_id == 1
