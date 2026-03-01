from faststream import FastStream
from faststream.exceptions import AckMessage
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@broker.subscriber("test-subject", stream="test-stream")
async def handle(body):
    smth_processing(body)


def smth_processing(body):
    if True:
        raise AckMessage()


@app.after_startup
async def test_publishing():
    await broker.publish("Hello!", "test-subject")
