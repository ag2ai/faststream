from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)


@broker.subscriber("test-topic")
async def handle(
    name: str,
    user_id: int,
):
    assert name == "John"
    assert user_id == 1
