import io
import json
from typing import Any

import fastavro
from confluent_kafka.schema_registry import SchemaRegistryClient
from pydantic import BaseModel

from faststream import (
    BaseMiddleware,
    BatchPublishCommand,
    FastStream,
    PublishCommand,
)
from faststream.confluent import KafkaBroker, KafkaMessage

AVRO_SCHEMA = {
    "type": "record",
    "namespace": "example",
    "name": "Person",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
    ],
}
AVRO_SCHEMA_JSON = json.dumps(AVRO_SCHEMA)

registry = SchemaRegistryClient({"url": "http://localhost:8081"})
LATEST_SCHEMAS: dict[str, tuple[int, dict[str, Any]]] = {}
SCHEMAS_BY_ID: dict[int, dict[str, Any]] = {}


class Person(BaseModel):
    name: str
    age: int


def get_latest_schema(subject: str) -> tuple[int, dict[str, Any]]:
    """Return the latest schema id and definition registered for a subject."""
    if subject not in LATEST_SCHEMAS:
        latest = registry.get_latest_version(subject)
        if latest.schema_id is None or latest.schema.schema_str is None:
            msg = f"Subject {subject!r} has no registered schema"
            raise ValueError(msg)
        LATEST_SCHEMAS[subject] = (
            latest.schema_id,
            json.loads(latest.schema.schema_str),
        )
    return LATEST_SCHEMAS[subject]


def get_schema_by_id(schema_id: int) -> dict[str, Any]:
    """Return the schema definition registered under a specific id."""
    if schema_id not in SCHEMAS_BY_ID:
        schema = registry.get_schema(schema_id)
        if schema.schema_str is None:
            msg = f"Schema id {schema_id} has no definition"
            raise ValueError(msg)
        SCHEMAS_BY_ID[schema_id] = json.loads(schema.schema_str)
    return SCHEMAS_BY_ID[schema_id]


def encode_avro(value: Any, schema: dict[str, Any], schema_id: int) -> bytes:
    """Encode a value in the Confluent wire format."""
    if isinstance(value, BaseModel):
        value = value.model_dump()

    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, value)

    # Confluent wire format: magic byte + 4-byte schema id + Avro payload
    return b"\x00" + schema_id.to_bytes(4, "big") + buffer.getvalue()


def decode_avro(body: bytes) -> Any:
    """Decode a value from the Confluent wire format."""
    if body[:1] != b"\x00":
        msg = "Message is not in the Confluent wire format"
        raise ValueError(msg)

    schema_id = int.from_bytes(body[1:5], "big")
    schema = get_schema_by_id(schema_id)
    return fastavro.schemaless_reader(io.BytesIO(body[5:]), schema)


class SchemaRegistryMiddleware(BaseMiddleware):
    async def publish_scope(self, call_next, cmd: PublishCommand) -> Any:
        subject = f"{cmd.destination}-value"
        schema_id, schema = get_latest_schema(subject)

        if isinstance(cmd, BatchPublishCommand):
            cmd.batch_bodies = [
                encode_avro(body, schema, schema_id) for body in cmd.batch_bodies
            ]
        else:
            cmd.body = encode_avro(cmd.body, schema, schema_id)

        return await call_next(cmd)


def schema_registry_decoder(msg: KafkaMessage) -> Any:
    if isinstance(msg.body, list):
        return [decode_avro(body) for body in msg.body]
    return decode_avro(msg.body)


broker = KafkaBroker(
    middlewares=[SchemaRegistryMiddleware],
    decoder=schema_registry_decoder,
)
app = FastStream(broker)


@broker.subscriber("people")
async def handle_person(person: Person) -> None:
    print(f"Received {person.name}, age {person.age}")


@app.after_startup
async def publish_person() -> None:
    await broker.publish(Person(name="Ada", age=36), "people")
