from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

publisher = broker.publisher("another-topic")

@broker.subscriber("test-topic")
async def handle():
    await publisher.publish("Hi!")


@broker.subscriber("another-topic")
async def handle_next(msg: str):
    assert msg == "Hi!"
