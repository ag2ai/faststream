from faststream import FastStream, ContextRepo, Context
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")
app = FastStream(broker)


@broker.subscriber("test-channel")
async def handle(
    msg: str,
    secret_str: str = Context(),
):
    assert secret_str == "my-perfect-secret" # pragma: allowlist secret


@app.on_startup
async def set_global(context: ContextRepo):
    context.set_global("secret_str", "my-perfect-secret")
