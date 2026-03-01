from msgspec import field
from fast_depends.msgspec import MsgSpecSerializer

from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker(
    "localhost:9092",
    serializer=MsgSpecSerializer(),
)
app = FastStream(broker)


@broker.subscriber("test-channel", auto_offset_reset="earliest")
async def handle(
    name: str,
    user_id: int = field(name="userId"),
):
    assert name == "John"
    assert user_id == 1
