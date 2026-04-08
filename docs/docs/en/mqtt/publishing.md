---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publishing over MQTT

FastStream uses the same publishing patterns as in the [getting started guide](../getting-started/publishing/index.md){.internal-link}: `await broker.publish(...)`, `@broker.publisher(...)`, and publisher objects.

## `MQTTBroker.publish`

Key arguments:

| Argument | Description |
| -------- | ----------- |
| `message` | Body (`SendableMessage`): primitives, models, `bytes`, etc. |
| `topic` | Target topic (**must not** contain `+` or `#`). |
| `qos` | `QoS.AT_MOST_ONCE` (0), `AT_LEAST_ONCE` (1), or `EXACTLY_ONCE` (2). |
| `retain` | If true, the broker stores the last message for new subscribers. |
| `headers` | **MQTT 5.0 only** — maps to User Properties. |
| `correlation_id` | **MQTT 5.0 only** — Correlation Data (for tracing or paired replies). |
| `reply_to` | **MQTT 5.0 only** — Response Topic for request/reply style flows. |

MQTT 3.1.1 rejects `headers`, `correlation_id`, and `reply_to`; use MQTT 5.0 for metadata on the wire.

## Publisher objects

`broker.publisher("topic", qos=..., retain=..., headers=...)` returns an `MQTTPublisher` with the same semantics. Per-call `publish()` can override `qos` / `retain` / `headers` where applicable.

## Batching

MQTT has no batch publish in FastStream — calling batch APIs raises `FeatureNotSupportedException`.
