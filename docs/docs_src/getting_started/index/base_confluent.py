from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")

app = FastStream(broker)


@broker.subscriber("test")
async def base_handler(body):
    print(body)
