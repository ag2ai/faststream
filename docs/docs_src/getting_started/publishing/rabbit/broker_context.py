from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(msg: str):
    assert msg == "Hi!"


@app.after_startup
async def test():
    async with RabbitBroker("amqp://guest:guest@localhost:5672/") as br:
        await br.publish("Hi!", queue="test-queue")
