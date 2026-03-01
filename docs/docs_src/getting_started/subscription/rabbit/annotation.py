from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(
    name: str,
    user_id: int,
):
    assert name == "John"
    assert user_id == 1
