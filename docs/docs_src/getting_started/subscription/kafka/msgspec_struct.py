from msgspec import field, Struct
from fast_depends.msgspec import MsgSpecSerializer

from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker(
    "localhost:9092",
    serializer=MsgSpecSerializer(),
)
app = FastStream(broker)


class UserInfo(Struct):
    name: str
    user_id: int = field(name="userId")


@broker.subscriber("test-channel")
async def handle(
    user: UserInfo,
):
    assert user.name == "John"
    assert user.user_id == 1
