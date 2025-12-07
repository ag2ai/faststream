from typing import Annotated
from faststream import Context, FastStream
from faststream.context import ContextRepo
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker, context=ContextRepo({
    "secret": "1"
}))

@broker.subscriber("test-queue")
async def handle(
    secret: Annotated[int, Context()],
):
    assert secret == "1"

@broker.subscriber("test-queue2")
async def handle_int(
    secret: Annotated[int, Context(cast=True)],
):
    assert secret == 1
