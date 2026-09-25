from faststream import FastStream
from faststream.kafka import KafkaBroker

broker_1 = KafkaBroker("localhost:9092")
broker_2 = KafkaBroker("localhost:9093")

app = FastStream(broker_1, broker_2)


@broker_1.subscriber("incoming")
async def from_first(msg: str) -> None:
    # Bridge the message from the first cluster to the second one
    await broker_2.publish(msg, "outgoing")


@broker_2.subscriber("outgoing")
async def from_second(msg: str) -> None:
    print(f"Received on the second cluster: {msg}")
