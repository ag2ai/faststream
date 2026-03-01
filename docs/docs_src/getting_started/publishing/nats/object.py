from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)

publisher = broker.publisher("another-subject")

@publisher
@broker.subscriber("test-subject")
async def handle() -> str:
    return "Hi!"


@broker.subscriber("another-subject")
async def handle_next(msg: str):
    assert msg == "Hi!"
