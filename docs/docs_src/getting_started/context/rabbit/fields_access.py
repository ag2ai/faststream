from typing import Annotated
from faststream import Context, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

@broker.subscriber("test-queue")
async def handle(
    msg: RabbitMessage,
    correlation_id: Annotated[str, Context("message.correlation_id")],
    user_header: Annotated[str, Context("message.headers.user")],
):
    assert msg.correlation_id == correlation_id
    assert msg.headers["user"] == user_header
