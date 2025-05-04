from faststream import Context, FastStream, apply_types
from faststream.kafka import KafkaBroker
from faststream.kafka.annotations import ContextRepo, KafkaMessage

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("test-topic")
async def handle(
    msg: str,
    message: KafkaMessage,
    context: ContextRepo,
):
    with context.scope("correlation_id", message.correlation_id):
        call()


@apply_types(context__=app.context)
def call(
    message: KafkaMessage,
    correlation_id=Context(),
):
    assert correlation_id == message.correlation_id
