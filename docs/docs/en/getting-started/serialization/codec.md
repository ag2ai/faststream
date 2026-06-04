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

```python
class CodecProto(Protocol):
    async def decode(self, msg: "StreamMessage[Any]") -> "DecodedMessage": ...
    async def encode(
        self,
        cmd: "PublishCommand",
        serializer: "SerializerProto | None" = None,
    ) -> tuple[bytes, str | None]: ...
```

- **`decode`** — receives a `StreamMessage` with raw bytes in `msg.body` and returns the decoded Python value. You can mutate `msg.body` before delegating to `decode_message`.
- **`encode`** — receives a `PublishCommand` with the outgoing message body and metadata, and an optional serializer. Returns a `(bytes, content_type)` tuple.
- **`cmd.destination`** — the target topic, subject, or queue name. Useful for codecs that need destination-specific behavior (e.g. Schema Registry topic-to-schema resolution).

If no codec is set, `DefaultCodec` is used automatically. It handles JSON objects, plain text, and raw bytes without any configuration.

## Compression Example

A Gzip codec that compresses outgoing messages and decompresses incoming ones:

=== "AIOKafka"
    ```python linenums="1" hl_lines="15-27 30"
    {!> docs_src/getting_started/serialization/codec_gzip_kafka.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="15-27 30"
    {!> docs_src/getting_started/serialization/codec_gzip_confluent.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="15-27 30"
    {!> docs_src/getting_started/serialization/codec_gzip_rabbit.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="15-27 30"
    {!> docs_src/getting_started/serialization/codec_gzip_nats.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="15-27 30"
    {!> docs_src/getting_started/serialization/codec_gzip_redis.py !}
    ```

=== "MQTT"
    ```python linenums="1" hl_lines="15-27 30"
    {!> docs_src/getting_started/serialization/codec_gzip_mqtt.py !}
    ```

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
