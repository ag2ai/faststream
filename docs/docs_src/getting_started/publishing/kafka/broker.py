from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("test-topic")
async def handle():
    await broker.publish("Hi!", topic="another-topic")


@broker.subscriber("another-topic")
async def handle_next(msg: str):
    assert msg == "Hi!"


@app.after_startup
async def test():
    await broker.publish("", topic="test-topic")
