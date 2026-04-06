---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Message information and serialization

FastStream wraps each incoming `zmqtt.Message` in `MQTTMessage`, which extends the generic [`StreamMessage`](../getting-started/context.md#existing-fields){.internal-link}.

## Fields on `MQTTMessage`

Typical fields used in handlers:

| Field | Meaning |
| ----- | ------- |
| `body` | Decoded payload (`bytes` or deserialized JSON / text depending on `content_type` and decoder). |
| `headers` | MQTT 5.0 **User Properties** as `dict[str, str]`. Empty for 3.1.1. |
| `content_type` | From MQTT 5.0 `Content Type` property, if present. |
| `reply_to` | **Response Topic** (MQTT 5.0), used for RPC replies. |
| `correlation_id` | **Correlation Data** decoded as text (MQTT 5.0). |
| `raw_message` | The original `zmqtt.Message` (topic, QoS, retain, `properties`). |

Access via a parameter, [`Context`](../getting-started/context.md){.internal-link}, or `Annotated` shortcuts, same as other brokers.

```python
from faststream.mqtt.annotations import MQTTMessage

@broker.subscriber("devices/+/status")
async def handle(msg: MQTTMessage):
    props = msg.raw_message.properties  # MQTT 5.0 only
    ...
```

## Serialization pipeline

Serialization follows the global FastStream rules ([custom serialization](../getting-started/serialization/index.md){.internal-link}):

1. **Encoding (publish)** — `encode_message` turns Python values into `bytes` and may set a logical content type (`application/json`, `text/plain`, etc.).
2. **MQTT 5.0** — that content type is written to `PublishProperties.content_type`, and the payload is sent as raw bytes.
3. **Decoding (consume)** — `MQTTParserV5.decode_message` uses `content_type` and the body to decode JSON or text; MQTT 3.1.1 uses heuristics on the raw payload (JSON first, then UTF-8 text, else `bytes`).

### User Properties as application headers

For MQTT 5.0, **`headers` in FastStream are User Properties** on the PUBLISH packet. They are string key/value pairs only—if you need binary metadata, encode it (for example Base64) or use the payload.

Custom or framework-specific metadata should use **`headers`** (User Properties). Protocol-level fields such as **Response Topic**, **Correlation Data**, and **Content Type** are exposed as dedicated attributes on `MQTTMessage`, not duplicated inside `headers`.

### Advanced: direct property access

Anything not exposed on `MQTTMessage` can still be read from `msg.raw_message.properties` (a `PublishProperties` instance) on MQTT 5.0, for example **message expiry interval**, **topic alias**, or additional spec fields supported by zmqtt.
