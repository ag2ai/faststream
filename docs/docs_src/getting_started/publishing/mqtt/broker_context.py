from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)


@broker.subscriber("test-topic")
async def handle(msg: str):
    assert msg == "Hi!"


@app.after_startup
async def test():
    async with MQTTBroker("localhost", port=1883) as br:
        await br.publish("Hi!", topic="test-topic")
