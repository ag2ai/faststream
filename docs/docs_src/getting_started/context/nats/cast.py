from faststream import Context, FastStream
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)
app.context.set_global("secret", "1")

@broker.subscriber("test-subject")
async def handle(
    secret: int = Context(),
):
    assert secret == "1"

@broker.subscriber("test-subject2")
async def handle_int(
    secret: int = Context(cast=True),
):
    assert secret == 1
