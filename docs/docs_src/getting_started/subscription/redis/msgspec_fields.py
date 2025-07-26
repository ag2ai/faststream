from msgspec import field
from fast_depends.msgspec import MsgSpecSerializer

from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker(
    "redis://localhost:6379",
    serializer=MsgSpecSerializer(),
)
app = FastStream(broker)


@broker.subscriber("test-channel")
async def handle(
    name: str,
    user_id: int = field(name="userId"),
):
    assert name == "John"
    assert user_id == 1
