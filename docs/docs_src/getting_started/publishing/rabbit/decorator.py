from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
@broker.publisher("another-queue")
async def handle() -> str:
    return "Hi!"


@broker.subscriber("another-queue")
async def handle_next(msg: str):
    assert msg == "Hi!"


@app.after_startup
async def test():
    await broker.publish("", queue="test-queue")
