from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.nats import NatsBroker

kafka_broker = KafkaBroker("localhost:9092")
nats_broker = NatsBroker("nats://localhost:4222")

app = FastStream(kafka_broker, nats_broker)


@kafka_broker.subscriber("incoming")
@nats_broker.publisher("outgoing")
async def from_kafka(msg: str) -> str:
    # Bridge the message from Kafka to NATS
    return msg


@nats_broker.subscriber("outgoing")
async def from_nats(msg: str) -> None:
    print(f"Received from NATS: {msg}")
