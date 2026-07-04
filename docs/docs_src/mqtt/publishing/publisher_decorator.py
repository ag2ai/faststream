from faststream import FastStream
from faststream.mqtt import MQTTBroker

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.publisher("processed")
@broker.subscriber("raw")
async def normalize(body: str) -> str:
    return body.upper()


@broker.subscriber("processed")
async def consume_processed(body: str) -> None:
    print(body)
