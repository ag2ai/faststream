from typing import Annotated
from faststream import Context, ContextRepo
from faststream.rabbit import RabbitBroker

broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    context=ContextRepo({"secret_str": "my-perfect-secret"}),
)

@broker.subscriber("test-queue")
async def handle(
    secret_str: Annotated[str, Context()],
):
    assert secret_str == "my-perfect-secret"
