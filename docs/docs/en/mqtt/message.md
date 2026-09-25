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

```python linenums="1" hl_lines="1 3"
{! docs_src/mqtt/message/fields.py [ln:9-12] !}
```

## Topic Path Access

MQTT topic filters support `+` (single level) and `#` (multi level) wildcards. **FastStream** lets you **capture** single-level matches by naming them in the subscriber topic template and reading them back via `Path` (a shortcut for `#!python Context("message.path.*")`).

{% raw %}
| Syntax | Replaces | Captures | Placement constraint |
| ------ | -------- | -------- | -------------------- |
| `"{name}"`, `f"{{name}}"` | `+` | One topic level as `#!python str` | Must occupy a whole topic level (surrounded by `/` or string boundaries). |
| `"{{name}}"`, `f"{{{{name}}}}"` | `{name}` | No capture | Allows braces to be treated as literal characters. |
{% endraw %}

### Single-level capture

```python linenums="1" hl_lines="1 2 5"
{! docs_src/mqtt/message/path.py [ln:1,8-13] !}
```

### Literal braces

MQTT topics may contain `{` and `}` as regular characters. Escape them by doubling braces so FastStream does not treat them as path parameters:

```python linenums="1" hl_lines="1"
{! docs_src/mqtt/message/literal_braces.py [ln:8-10] !}
```

For f-strings, double the escaping because Python consumes one brace level first:

```python linenums="1" hl_lines="1 4"
{! docs_src/mqtt/message/literal_braces.py [ln:13-17] !}
```

Both examples subscribe to the literal MQTT topic `/root/{braced}`.

### Multi-level topics

`#` subscriptions are supported as raw MQTT topic filters, but they are not captured through `Path`. Use `MQTTMessage.raw_message.topic` when you need the full topic.

```python linenums="1" hl_lines="1 3 7"
{! docs_src/mqtt/message/wildcard.py [ln:3,8-13] !}
```

### Validation

Templates that violate MQTT topic rules are rejected at subscriber creation with `SetupError`:

- `"/pre{name}/x"` or `"/{name}post/x"` — `{name}` does not occupy a whole topic level.
- `"/{id}/x/{id}"` — duplicated parameter name.

Raw MQTT `+` and `#` wildcards may be used alongside captured `{name}` levels. Only named single-level parameters are captured; use `MQTTMessage.raw_message.topic` for full-topic access when `#` is involved.

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
