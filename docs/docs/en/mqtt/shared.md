---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Shared subscriptions

Some MQTT brokers support **shared subscriptions** so that each message is delivered to **one** consumer in a group (load balancing). The canonical topic form is:

```text
$share/<group_name>/<topic_filter>
```

## FastStream API

Pass **`shared="group_name"`** to `subscriber`. FastStream prepends `$share/<group_name>/` to your topic filter.

```python
from faststream.mqtt import MQTTBroker, QoS

broker = MQTTBroker("localhost", version="5.0")


@broker.subscriber("workers/jobs/#", qos=QoS.AT_LEAST_ONCE, shared="pool-a")
async def handle_job(cmd: dict) -> None:
    ...
```

Multiple application instances using the same `shared` name and topic filter compete for messages the way the broker defines for shared subscriptions.

## Broker support

Shared subscriptions are a **server feature**. If your broker does not implement them, subscription may fail or behave like a literal topic starting with `$share/`. Consult your broker’s documentation.

## Routers and prefixes

`MQTTRouter` / `include_router(..., prefix=...)` **doesn\`t work** for now with `prefix` and `shared` combination. You receive `MQTTInvalidTopicError` from `zmqtt` if try to combine it.
