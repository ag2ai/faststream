from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@broker.subscriber("test-subject")
async def handle(msg: str):
    assert msg == "Hi!"


@app.after_startup
async def test():
    async with NatsBroker("nats://localhost:4222") as br:
        await br.publish("Hi!", subject="test-subject")
