from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)


@broker.subscriber("test-topic")
@broker.publisher("another-topic")
async def handle() -> str:
    return "Hi!"


@broker.subscriber("another-topic")
async def handle_next(msg: str):
    assert msg == "Hi!"


@app.after_startup
async def test():
    await broker.publish("", topic="test-topic")
