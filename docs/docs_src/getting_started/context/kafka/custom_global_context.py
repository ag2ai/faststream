from typing import Annotated
from faststream import FastStream, ContextRepo, Context
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@broker.subscriber("test-topic")
async def handle(
    secret_str: Annotated[str, Context()],
):
    assert secret_str == "my-perfect-secret"

@app.on_startup
async def set_global(context: ContextRepo):
    context.set_global("secret_str", "my-perfect-secret")
