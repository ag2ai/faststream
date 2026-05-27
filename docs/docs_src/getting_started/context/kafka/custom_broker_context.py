from typing import Annotated
from faststream import Context, ContextRepo
from faststream.kafka import KafkaBroker

broker = KafkaBroker(
    "localhost:9092",
    context=ContextRepo({"secret_str": "my-perfect-secret"}),
)

@broker.subscriber("test-topic")
async def handle(
    secret_str: Annotated[str, Context()],
):
    assert secret_str == "my-perfect-secret"
