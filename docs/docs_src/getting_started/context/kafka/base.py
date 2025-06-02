from faststream import Context, FastStream
from faststream.kafka import KafkaBroker, KafkaMessage

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("test")
async def base_handler(
    body: str,
    message: KafkaMessage = Context(),  # get access to raw message
):
    ...
