---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# MQTT 3.1.1 and MQTT 5.0 in FastStream

FastStream selects the wire protocol with `MQTTBroker(..., version="3.1.1" | "5.0")`. The same Python API is available for both; features that require MQTT 5.0 raise `FeatureNotSupportedException` when `version="3.1.1"`.

## Feature comparison

| Capability | MQTT 3.1.1 | MQTT 5.0 |
| ---------- | ----------- | -------- |
| **User properties / headers** | Not available | `headers=` on `publish` / publisher maps to **User Properties** |
| **`correlation_id`**, **`reply_to`** | Not available on the wire | Mapped to **Correlation Data** and **Response Topic** in `PublishProperties` |
| **`broker.request()`** | Supported only with an **explicit** `reply_to` topic (see [RPC](rpc.md){.internal-link}) | Uses native MQTT 5.0 request/response (zmqtt generates a private reply topic) |

## How FastStream maps metadata (MQTT 5.0)

Outgoing messages use `zmqtt.PublishProperties`:

- Serializer output sets **`content_type`** when the encoded body is JSON or plain text (see [Message & serialization](message.md){.internal-link}).
- **`headers`** → **User Properties** (string key/value pairs).
- **`correlation_id`** → **Correlation Data** (UTF-8 bytes of the string).
- **`reply_to`** → **Response Topic** (must be a valid publish topic; no wildcards).

Incoming `MQTTMessage` values reverse this mapping for MQTT v5.0: user properties populate `message.headers`, and standard properties populate `content_type`, `reply_to`, and `correlation_id`.

## MQTT 3.1.1 limitations

With `version="3.1.1"`:

- Passing `headers`, `correlation_id`, or `reply_to` to `MQTTBroker.publish` / `MQTTPublisher.publish` raises **`FeatureNotSupportedException`**.
- Handlers still receive `MQTTMessage` with empty `headers`, no `correlation_id`, and no `reply_to` from the wire (only the payload is meaningful).
- RPC-style calls use the explicit reply-topic flow described in [Request / response](rpc.md){.internal-link}.

If you need custom metadata or correlation on the wire, use **MQTT 5.0**.
