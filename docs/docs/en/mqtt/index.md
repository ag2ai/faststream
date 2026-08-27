---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# MQTT routing

!!! note ""
    **FastStream** MQTT support is implemented on top of [**zmqtt**](https://pypi.org/project/zmqtt/){.external-link target="_blank"} — a pure `asyncio` MQTT 3.1.1 / 5.0 client with no extra runtime dependencies. You can use the underlying `zmqtt.MQTTClient` via the broker connection when you need APIs not wrapped by FastStream.

## Why MQTT

[MQTT](https://mqtt.org/){.external-link target="_blank"} is a lightweight publish/subscribe protocol designed for constrained networks and high fan-out. Messages are addressed by **topic** strings; brokers route publishes to subscribers whose **topic filters** match (including `+` and `#` wildcards).

Compared to Kafka or RabbitMQ, MQTT emphasizes simple topic namespaces, optional persistent sessions, and QoS levels built into the protocol. Choose MQTT when your infrastructure or devices already speak MQTT, or when you want broker-mediated pub/sub without managing exchanges or partitions yourself.

## FastStream `MQTTBroker`

Import the broker and optional helpers from `#!python faststream.mqtt`:

```python linenums="1" hl_lines="4 8-13 20"
{! docs_src/mqtt/basic.py !}
```

### Connection parameters

The broker constructor mirrors common `zmqtt.MQTTClient` options:

| Parameter | Role |
| --------- | ---- |
| `url` | Broker URL. `mqtt://` uses plain TCP and port `1883`; `mqtts://` uses TLS and port `8883`. Username and password can be included in the URL. |
| `host`, `port` | Legacy parameters retained for backward compatibility. |
| `version` | `#!python "3.1.1"` or `#!python "5.0"` — selects protocol features and how FastStream maps metadata (see [MQTT versions](versions.md){.internal-link}). |
| `client_id` | Client identity string. |
| `security` | Pass `SASLPlaintext(username, password)` or `BaseSecurity(ssl_context)` for credentials and TLS (see [Security](security.md){.internal-link}). |
| `keepalive`, `clean_session` | Session behaviour. |
| `will` | Optional `Will` (from `#!python faststream.mqtt`) published by the broker after an unexpected disconnect. `WillProperties` are supported with MQTT 5.0. |
| `reconnect` | Optional `ReconnectConfig` (from `#!python faststream.mqtt`) for automatic reconnect with backoff. |
| `on_connection_recovery_failed` | Optional async callback invoked after a running connection cannot be restored. FastStream passes the callback directly to `zmqtt`. |
| `session_expiry_interval` | MQTT 5.0 session expiry (seconds). |
| `session_replay_buffer_size` | Maximum unmatched messages held while a resumed persistent session waits for local subscriptions. The default is `1000`; `0` is unbounded. |
| `session_replay_timeout` | Seconds to wait for local subscriptions before dropping unmatched replay messages without acknowledging them. The default is `30`. |
| `mqtt_connect_timeout` | Seconds to wait for the broker's CONNACK during the MQTT connect handshake (default `30`); raises `MQTTTimeoutError` (from `#!python zmqtt`), and is retried when `reconnect` is enabled. |

Routers reuse the same API via `MQTTRouter` / `MQTTRoute` (see [routers](../getting-started/routers/index.md){.internal-link}).

### Terminal connection recovery failure

When `zmqtt` exhausts the configured runtime reconnect attempts, it invokes the
user-provided `on_connection_recovery_failed` callback and raises the terminal
error from active subscription iterators. FastStream stops the failed consumer
task instead of restarting it with the same disconnected client.

FastStream does not stop the application or create a new client automatically.
The broker remains part of the running application, and `await broker.ping()`
returns `False`. Use the callback or your application's health check to report
the failure and let your deployment policy decide whether to restart the process.

### Persistent-session startup replay

With a stable `client_id`, `clean_session=False`, and a positive MQTT 5.0
`session_expiry_interval`, a broker can replay queued messages immediately after
CONNACK, before FastStream starts its local subscribers. `zmqtt` temporarily holds
those messages in its session replay buffer and routes them after the matching
subscriptions are ready. Use `session_replay_buffer_size` and
`session_replay_timeout` to size that startup window for the expected backlog.

## Where to read next

- [Publishing](publishing.md){.internal-link} — `qos`, `retain`, MQTT 5.0 headers and reply topics
- [Message object](message.md){.internal-link} — body, headers, `correlation_id`, serialization, topic path capture
- [Acknowledgement](ack.md){.internal-link} — `AckPolicy`, QoS, and manual ack
- [Request / response](rpc.md){.internal-link} — `broker.request()` and handler replies
- [MQTT 3.1.1 vs 5.0](versions.md){.internal-link} — feature matrix
- [Shared subscriptions](shared.md){.internal-link} — load balancing with `$share`
- [Security](security.md){.internal-link} — TLS and SASL-style username/password
