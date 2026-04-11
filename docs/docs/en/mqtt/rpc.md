---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Request / response over MQTT

FastStream mirrors the [RabbitMQ RPC](../rabbit/rpc.md){.internal-link} style: `await broker.request(...)` blocks until a reply arrives, and subscribers that return a value publish the response to the caller’s reply address.

## MQTT 5.0 (recommended)

Set `version="5.0"`. The producer uses zmqtt’s **`request()`**, which allocates a private **response topic**, subscribes, publishes with **Correlation Data**, and waits for one matching PUBLISH.

- You usually **do not** pass `reply_to` — it is generated for you.
- **`correlation_id`** and **`headers`** are supported on the request and are carried in `PublishProperties`.

The handler side receives `MQTTMessage.reply_to` and `correlation_id` populated from **Response Topic** and **Correlation Data**. When your handler returns a value (or a [`Response`](../rabbit/rpc.md){.internal-link} object), FastStream publishes the reply to that response topic with the same correlation.

```python
from faststream import FastStream
from faststream.mqtt import MQTTBroker, MQTTMessage, QoS

broker = MQTTBroker("localhost", version="5.0")
app = FastStream(broker)


@broker.subscriber("rpc/echo", qos=QoS.AT_LEAST_ONCE)
async def echo(body: str) -> str:
    return body


@app.after_startup
async def demo() -> None:
    reply: MQTTMessage = await broker.request("hello", "rpc/echo", timeout=5.0)
    assert reply.body  # decoded payload
```

### `MQTTResponse`

For MQTT-specific reply options, return `MQTTResponse` from `faststream.mqtt.response` to set **`qos`** and **`retain`** on the outgoing reply (in addition to `body`, `headers`, and `correlation_id`).

## MQTT 3.1.1

There are no **Response Topic** or **Correlation Data** properties. FastStream instead requires a **stable reply topic** known to both sides:

1. Pass **`reply_to="my/shared/reply/topic"`** to `broker.request(...)`.
2. The client subscribes to that topic, publishes the request, and waits for the first message on the reply topic.
3. Your service must publish the response to **`reply_to`** (for example with `@broker.publisher` or explicit `publish`).

If `reply_to` is omitted, `request()` raises **`FeatureNotSupportedException`**.

## Disabling automatic replies

Set `#!python @broker.subscriber(..., no_reply=True)` if the handler should not send an RPC reply (same as RabbitMQ).
