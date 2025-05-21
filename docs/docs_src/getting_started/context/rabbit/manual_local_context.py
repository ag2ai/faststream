from faststream import Context, FastStream, apply_types
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


@broker.subscriber("test-queue")
async def handle(
    msg: str,
    message: RabbitMessage,
):
    tag = app.context.set_local("correlation_id", message.correlation_id)
    call(tag)


@apply_types(context__=app.context)
def call(
    tag,
    message: RabbitMessage,
    correlation_id=Context(),
):
    assert correlation_id == message.correlation_id
    app.context.reset_local("correlation_id", tag)
