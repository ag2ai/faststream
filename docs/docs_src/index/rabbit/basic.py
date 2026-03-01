from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

@broker.subscriber("in-queue")
@broker.publisher("out-queue")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
