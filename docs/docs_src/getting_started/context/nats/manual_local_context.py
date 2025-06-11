from faststream import Context, FastStream, apply_types, context
from faststream.nats import NatsBroker
from faststream.nats.annotations import NatsMessage

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@broker.subscriber("test-subject")
async def handle(
    msg: str,
    message: NatsMessage,
):
    tag = context.set_local("correlation_id", message.correlation_id)
    call(tag)


@apply_types
def call(
    tag,
    message: NatsMessage,
    correlation_id: str = Context(),
):
    assert correlation_id == message.correlation_id
    context.reset_local("correlation_id", tag)
