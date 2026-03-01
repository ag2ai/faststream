from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

subscriber = broker.subscriber("test-queue")

@subscriber(
    filter=lambda msg: msg.content_type == "application/json",
)
async def handle(name: str, user_id: int):
    assert name == "John"
    assert user_id == 1

@subscriber
async def default_handler(msg: str):
    assert msg == "Hello, FastStream!"



@app.after_startup
async def test():
    await broker.publish(
        {"name": "John", "user_id": 1},
        queue="test-queue",
    )

    await broker.publish(
        "Hello, FastStream!",
        queue="test-queue",
    )
