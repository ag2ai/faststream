from msgspec import field
from fast_depends.msgspec import MsgSpecSerializer

from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    serializer=MsgSpecSerializer(),
)
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(
    name: str,
    user_id: int = field(name="userId"),
):
    assert name == "John"
    assert user_id == 1
