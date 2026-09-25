from typing import Annotated
from faststream import FastStream, ContextRepo, Context
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", port=1883)
app = FastStream(broker)

@broker.subscriber("test-topic")
async def handle(
    secret_str: Annotated[str, Context()],
):
    assert secret_str == "my-perfect-secret"

@app.on_startup
async def set_global(context: ContextRepo):
    context.set_global("secret_str", "my-perfect-secret")
