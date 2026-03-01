from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@broker.subscriber("test-subject")
async def handle(
    name: str,
    user_id: int,
):
    assert name == "John"
    assert user_id == 1
