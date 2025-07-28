from msgspec import field, Struct
from fast_depends.msgspec import MsgSpecSerializer

from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker(
    "nats://localhost:4222",
    serializer=MsgSpecSerializer(),
)
app = FastStream(broker)


class UserInfo(Struct):
    name: str
    user_id: int = field(name="userId")


@broker.subscriber("test-subject")
async def handle(
    user: UserInfo,
):
    assert user.name == "John"
    assert user.user_id == 1
