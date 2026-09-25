from typing import Annotated
from faststream import Context, FastStream
from faststream.context import ContextRepo
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker, context=ContextRepo({
    "secret": "1"
}))

@broker.subscriber("test-topic")
async def handle(
    secret: Annotated[int, Context()],
):
    assert secret == "1"

@broker.subscriber("test-topic2")
async def handle_int(
    secret: Annotated[int, Context(cast=True)],
):
    assert secret == 1
