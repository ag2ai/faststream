---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Custom Codec

A codec provides a unified interface for both encoding (publishing) and decoding (consuming) messages. Unlike the older `decoder=` approach, a codec handles both directions in a single class.

## Protocol

Implement the `CodecProto` interface to create a custom codec:

::: faststream._internal.parser.CodecProto

- **`decode`** — receives a `StreamMessage` with raw bytes in `msg.body` and returns the decoded Python value.
- **`encode`** — receives a `PublishCommand` containing the message body, destination, and headers. Returns an `EncodedMessage` dataclass with `body: bytes` and `content_type: str | None`. Access the payload via `cmd.body` and the target topic/subject/queue via `cmd.destination`.

If no codec is set, `DefaultCodec` is used automatically. It handles JSON objects, plain text, and raw bytes.

## Example: Schema Registry

A Confluent Avro codec that encodes and decodes messages using the [Confluent wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format){target="_blank"} (magic byte + schema ID + Avro payload). Requires `fastavro` and `confluent-kafka`:

```bash
pip install fastavro confluent-kafka
```

```python linenums="1" hl_lines="22-66 68-76"
{!> docs_src/getting_started/serialization/codec_schema_registry_kafka.py !}
```

!!! note
    The codec fetches and caches schemas from the registry at startup and on first encounter. The `subject` follows Confluent's naming convention: `{topic}-value`.

## Priority

You can set a codec at the broker level or override it per subscriber. The subscriber-level codec always wins:

```python
broker = KafkaBroker(codec=BrokerCodec())

@broker.subscriber("test", codec=SubscriberCodec())  # ← this wins
async def handle(body: str) -> None:
    ...

# If no codec is set at any level, DefaultCodec is used (JSON/text/bytes)
```

## Compatibility

- **`codec=` and `parser=`** work together. The parser controls how the raw broker message is parsed into a `StreamMessage`; the codec then decodes or encodes the body.
- **`codec=` and `decoder=`** cannot be used together. Specifying both raises a `ValueError`.
- For the legacy `decoder=` approach, see [Custom Decoder](./decoder.md){.internal-link}.
