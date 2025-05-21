from faststream import Context, FastStream, apply_types
from faststream.nats import NatsBroker
from faststream.nats.annotations import NatsMessage

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@broker.subscriber("test-subject")
async def handle(
    msg: str,
    message: NatsMessage,
):
    tag = app.context.set_local("correlation_id", message.correlation_id)
    call(tag)


@apply_types(context__=app.context)
def call(
    tag,
    message: NatsMessage,
    correlation_id=Context(),
):
    assert correlation_id == message.correlation_id
    app.context.reset_local("correlation_id", tag)
