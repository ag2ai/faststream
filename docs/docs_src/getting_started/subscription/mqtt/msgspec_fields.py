from msgspec import field
from fast_depends.msgspec import MsgSpecSerializer

from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker(
    "localhost",
    port=1883,
    serializer=MsgSpecSerializer(),
)
app = FastStream(broker)


@broker.subscriber("test-topic")
async def handle(
    name: str,
    user_id: int = field(name="userId"),
):
    assert name == "John"
    assert user_id == 1
