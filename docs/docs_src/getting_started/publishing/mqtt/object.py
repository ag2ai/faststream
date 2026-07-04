from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)

publisher = broker.publisher("another-topic")

@publisher
@broker.subscriber("test-topic")
async def handle() -> str:
    return "Hi!"


@broker.subscriber("another-topic")
async def handle_next(msg: str):
    assert msg == "Hi!"
