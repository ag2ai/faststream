from typing import Annotated
from faststream import Context, ContextRepo
from faststream.redis import RedisBroker

broker = RedisBroker(
    "redis://localhost:6379",
    context=ContextRepo({"secret_str": "my-perfect-secret"}),
)

@broker.subscriber("test-channel")
async def handle(
    secret_str: Annotated[str, Context()],
):
    assert secret_str == "my-perfect-secret"
