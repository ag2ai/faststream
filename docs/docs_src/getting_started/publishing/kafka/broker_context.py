from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("test-topic")
async def handle(msg: str):
    assert msg == "Hi!"


@app.after_startup
async def test():
    async with KafkaBroker("localhost:9092") as br:
        await br.publish("Hi!", topic="test-topic")
