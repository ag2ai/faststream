from faststream import Context, FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)
app.context.set_global("secret", "1")

@broker.subscriber("test-topic")
async def handle(
    secret: int = Context(),
):
    assert secret == "1"

@broker.subscriber("test-topic2")
async def handle_int(
    secret: int = Context(cast=True),
):
    assert secret == 1
