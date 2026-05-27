from typing import Annotated
from faststream import Context, ContextRepo
from faststream.nats import NatsBroker

broker = NatsBroker(
    "nats://localhost:4222",
    context=ContextRepo({"secret_str": "my-perfect-secret"}),
)

@broker.subscriber("test-subject")
async def handle(
    secret_str: Annotated[str, Context()],
):
    assert secret_str == "my-perfect-secret"
