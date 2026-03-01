from typing import Annotated
from faststream import Context, FastStream
from faststream.context import ContextRepo
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker, context=ContextRepo({
    "secret": "1"
}))

@broker.subscriber("test-subject")
async def handle(
    secret: Annotated[int, Context()],
):
    assert secret == "1"

@broker.subscriber("test-subject2")
async def handle_int(
    secret: Annotated[int, Context(cast=True)],
):
    assert secret == 1
