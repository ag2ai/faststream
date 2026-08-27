---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Concurrency

By default, a **Kafka**, **Confluent**, **NATS**, **Redis** or **MQTT** subscriber processes one message at a time: the next message is not handled until the current one is finished. `max_workers` lifts that restriction.

**RabbitMQ** works the other way around — it is already concurrent and needs to be *limited* instead. See [Concurrency and Prefetch](../../rabbit/prefetch.md).

## `max_workers`

`max_workers` is the **number of concurrent handler invocations** a single subscriber may run:

```python hl_lines="3"
@broker.subscriber(
    "test",
    max_workers=4,
)
async def handle(msg): ...
```

With `max_workers=1` (the default) **FastStream** processes one message at a time per subscriber. With `max_workers=N`, up to `N` invocations of the handler can run at the same time.

!!! note
    `max_workers` is not available on the RabbitMQ subscriber. RabbitMQ subscribers do not serialize handler invocations in the first place, so there is nothing for `max_workers` to lift — what you tune there is `prefetch_count`, which bounds how many messages the broker may have in flight. See [Concurrency and Prefetch](../../rabbit/prefetch.md).

!!! note
    `max_workers` controls how many messages are processed **at once**, not how many are fetched from the broker. Those are separate concerns, and some brokers expose their own knob for the second one:

    - **Kafka / Confluent** — the consumer pulls in batches; tune `max_poll_records` (and friends) to control how many records are fetched per poll. See [Kafka Subscriber concurrent processing](../../kafka/Subscriber/index.md#concurrent-processing) for the interaction with `AckPolicy`.
    - **NATS, Redis, MQTT** — only `max_workers` applies; there is no broker-side prefetch equivalent.
