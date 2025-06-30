from faststream import Context, FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)
app.context.set_global("secret", "1")

@broker.subscriber("test-queue")
async def handle(
    secret: int = Context(),
):
    assert secret == "1"

@broker.subscriber("test-queue2")
async def handle_int(
    secret: int = Context(cast=True),
):
    assert secret == 1
