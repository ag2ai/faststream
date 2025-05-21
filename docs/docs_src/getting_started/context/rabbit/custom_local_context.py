from faststream import Context, FastStream, apply_types
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import ContextRepo, RabbitMessage

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(
    msg: str,
    message: RabbitMessage,
    context: ContextRepo,
):
    with context.scope("correlation_id", message.correlation_id):
        call()


@apply_types(context__=app.context)
def call(
    message: RabbitMessage,
    correlation_id=Context(),
):
    assert correlation_id == message.correlation_id
