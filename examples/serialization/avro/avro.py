import io

import fastavro

from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker, KafkaMessage

broker = KafkaBroker()
app = FastStream(broker)

person_schema = {
    "type": "record",
    "namespace": "Person",
    "name": "Person",
    "fields": [
        {"doc": "Name", "type": "string", "name": "name"},
        {"doc": "Age", "type": "int", "name": "age"},
    ],
}

person_schema = fastavro.schema.load_schema("person.avsc")

schema = fastavro.schema.parse_schema(person_schema)


async def decode_message(msg: KafkaMessage):
    bytes_reader = io.BytesIO(msg.body)
    msg_dict = fastavro.schemaless_reader(bytes_reader, schema)
    return msg_dict


@broker.subscriber("test", decoder=decode_message)
async def consume(name: str, age: int, logger: Logger):
    logger.info("%s: %s", name, age)


@app.after_startup
async def publish():
    msg = {"name": "John", "age": 25}
    bytes_writer = io.BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, msg)
    raw_bytes = bytes_writer.getvalue()

    await broker.publish(raw_bytes, "test")
