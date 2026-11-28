---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Confluent Schema Registry with Avro

The [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html){.external-link target="_blank"} stores message schemas and assigns each one a numeric id.
Kafka messages carry that id instead of the full schema, so producers and consumers can exchange compact Avro payloads and still agree on their structure.

**FastStream** does not hide the registry behind a broker option.
Instead, you keep the registry client in your application and plug it into the normal [encoder](../../getting-started/serialization/parser.md){.internal-link} and [decoder](../../getting-started/serialization/decoder.md){.internal-link} hooks.
The same pattern works for JSON Schema and Protobuf, and for registries other than Confluent.

## Install

```console
pip install "faststream[confluent]" "confluent-kafka[schemaregistry]" fastavro
```

`confluent-kafka[schemaregistry]` provides `SchemaRegistryClient`, and `fastavro` performs the Avro encoding and decoding.

## Register the schema

The example below expects the schema to be registered under the `people-value` subject.
Register it once, before starting the application:

```python
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

registry = SchemaRegistryClient({"url": "http://localhost:8081"})
schema_id = registry.register_schema(
    "people-value",
    Schema(AVRO_SCHEMA_JSON, schema_type="AVRO"),
)
```

The subject name follows the `<topic>-<key|value>` convention, so the `people` topic uses `people-value` for its values and `people-key` for its keys.
`AVRO_SCHEMA_JSON` is the schema defined in the next section.

## Schema, model, and registry lookups

```python linenums="1"
{! docs_src/confluent/schema_registry/app.py [ln:17-60] !}
```

The Avro schema is the contract on the wire.
The `Person` model is the contract in the handler; **FastStream** validates incoming values against it after the decoder returns a dictionary.

Both lookups are cached.
`get_latest_schema` is used by the encoder, and `get_schema_by_id` is used by the decoder.
The decoder fetches the schema by the id stored in the message, not the latest one, so old messages remain readable after a compatible schema change.

## Encode and decode

Confluent uses a small wire-format header before the Avro payload:

```text
0x00 | 4-byte schema id (big-endian) | Avro-encoded value
```

```python linenums="1"
{! docs_src/confluent/schema_registry/app.py [ln:63-83] !}
```

`encode_avro` accepts either a Pydantic model or a plain dictionary.
`decode_avro` reads the schema id from the message, looks up the matching schema, and passes both to `fastavro`.

!!! note
    The caches do not expire.
    If your schemas evolve while the service is running, use a TTL cache or clear the relevant entry after registering a new version.

## Encode outgoing messages

A middleware is the right place to encode every outgoing message, including replies and direct `broker.publish` calls.
`cmd.destination` is the topic the message is being published to, so the subject is derived from it.

```python linenums="1"
{! docs_src/confluent/schema_registry/app.py [ln:86-98] !}
```

Confluent's `KafkaPublishCommand` extends `BatchPublishCommand`, so the same branch handles `publish` and `publish_batch`.
Assigning through `batch_bodies` keeps every message in a batch and realigns any per-message keys.

## Decode incoming messages

The decoder receives a `KafkaMessage`.
For a batch subscriber, `msg.body` is a list of payloads, so decode each one separately.

```python linenums="1"
{! docs_src/confluent/schema_registry/app.py [ln:101-104] !}
```

Pass the middleware and decoder to the broker.
The middleware runs for every publisher, and the decoder runs for every subscriber.

## Run the app

```python linenums="1"
{! docs_src/confluent/schema_registry/app.py [ln:107-121] !}
```

The `after_startup` hook publishes one `Person`.
The middleware encodes it, the subscriber's decoder turns it back into a dictionary, and **FastStream** validates it as a `Person`.

## Notes

- **Blocking client.** `SchemaRegistryClient` performs synchronous HTTP requests. The caches above keep registry calls off the hot path, but a cache miss still blocks the event loop. In a high-throughput service, run cache misses in a worker thread, for example with `anyio.to_thread.run_sync`.
- **Keys.** A Kafka record has a value schema and, optionally, a key schema. The key subject is `<topic>-key`; apply the same encoder to `cmd.key` in the middleware if you publish keys.
- **Schema evolution.** The encoder uses `get_latest_version`, while the decoder uses the id from each message. Register a compatible new version, restart the service or clear `LATEST_SCHEMAS`, and old messages will still decode with their original schema.
- **Other registries.** The Avro calls are the only Confluent-specific part. Swap `fastavro` for your serializer and keep the same middleware and decoder shape for JSON Schema, Protobuf, or another registry.

For more serialization examples, see [Serialization examples](../../getting-started/serialization/examples.md#avro){.internal-link}.
