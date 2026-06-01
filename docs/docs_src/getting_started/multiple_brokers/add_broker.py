from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.nats import NatsBroker

kafka_broker = KafkaBroker("localhost:9092")
nats_broker = NatsBroker("nats://localhost:4222")

app = FastStream(kafka_broker)
app.add_broker(nats_broker)
