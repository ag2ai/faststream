from faststream import FastStream, ContextRepo, Context
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@broker.subscriber("test-subject")
async def handle(
    msg: str,
    secret_str: str = Context(),
):
    assert secret_str == "my-perfect-secret" # pragma: allowlist secret


@app.on_startup
async def set_global(context: ContextRepo):
    context.set_global("secret_str", "my-perfect-secret")
