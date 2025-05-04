from faststream import Context, FastStream, apply_types
from faststream.nats import NatsBroker
from faststream.nats.annotations import ContextRepo, NatsMessage

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)


@broker.subscriber("test-subject")
async def handle(
    msg: str,
    message: NatsMessage,
    context: ContextRepo,
):
    with context.scope("correlation_id", message.correlation_id):
        call()


@apply_types(context__=app.context)
def call(
    message: NatsMessage,
    correlation_id=Context(),
):
    assert correlation_id == message.correlation_id
