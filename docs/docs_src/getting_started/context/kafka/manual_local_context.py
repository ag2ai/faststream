from faststream import Context, FastStream, apply_types, ContextRepo
from faststream.kafka import KafkaBroker
from faststream.kafka.annotations import KafkaMessage

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("test-topic")
async def handle(
    msg: str,
    message: KafkaMessage,
    context: ContextRepo,
):
    tag = context.set_local("correlation_id", message.correlation_id)
    call(tag)


@apply_types(context__=app.context)
def call(
    tag,
    message: KafkaMessage,
    correlation_id=Context(),
):
    assert correlation_id == message.correlation_id
    app.context.reset_local("correlation_id", tag)
