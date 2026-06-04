import io
import json
import struct
from typing import TYPE_CHECKING, Any, Dict

import fastavro
from confluent_kafka.schema_registry import SchemaRegistryClient

from faststream import FastStream
from faststream.kafka import KafkaBroker

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import DecodedMessage
    from faststream.message import StreamMessage
    from faststream.response.response import PublishCommand

HEADER = struct.Struct(">bI")  # magic byte (0) + 4-byte schema ID


class SchemaRegistryCodec:
    def __init__(
        self,
        registry_url: str,
        topics: Dict[str, int],
    ) -> None:
        self._client = SchemaRegistryClient({"url": registry_url})
        self._schema_cache: Dict[int, Any] = {}
        self._topic_schemas: Dict[str, tuple[int, Any]] = {}

        for topic, version in topics.items():
            subject = f"{topic}-value"
            meta = self._client.get_version(subject, version)
            schema = fastavro.parse_schema(json.loads(meta.schema.schema_str))
            self._topic_schemas[topic] = (meta.schema_id, schema)
            self._schema_cache[meta.schema_id] = schema

    def _get_schema(self, schema_id: int) -> Any:
        if schema_id not in self._schema_cache:
            raw = self._client.get_schema(schema_id)
            self._schema_cache[schema_id] = fastavro.parse_schema(
                json.loads(raw.schema_str)
            )
        return self._schema_cache[schema_id]

    async def decode(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        schema_id = int.from_bytes(msg.body[1:5], byteorder="big")
        schema = self._get_schema(schema_id)
        decoded: dict[str, Any] = fastavro.schemaless_reader(
            io.BytesIO(msg.body[5:]), schema
        )
        return decoded  # type: ignore[return-value]

    async def encode(
        self,
        cmd: "PublishCommand",
        serializer: "SerializerProto | None" = None,
    ) -> tuple[bytes, str | None]:
        schema_id, schema = self._topic_schemas[cmd.destination]
        body = cmd.body
        data = body.model_dump(mode="json") if hasattr(body, "model_dump") else body
        buf = io.BytesIO()
        buf.write(HEADER.pack(0, schema_id))
        fastavro.schemaless_writer(buf, schema, data)
        return buf.getvalue(), "application/avro"


codec = SchemaRegistryCodec(
    registry_url="http://localhost:8081",
    topics={
        "orders": 1,
        "users": 2,
    },
)
broker = KafkaBroker(codec=codec)
app = FastStream(broker)


@broker.subscriber("orders")
async def handle_order(body: dict[str, Any]) -> None:
    ...


@broker.subscriber("users")
async def handle_user(body: dict[str, Any]) -> None:
    ...


@app.after_startup
async def test() -> None:
    await broker.publish({"order_id": "123", "amount": 99.99}, "orders")
    await broker.publish({"name": "John", "age": 25}, "users")
