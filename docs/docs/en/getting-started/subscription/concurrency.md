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

By default, a subscriber processes one message at a time: the next message is not handled until the current one is finished. `max_workers` lifts that restriction.

## `max_workers`

`max_workers` is the **number of concurrent handler invocations** a single subscriber may run. It is available on every broker's subscriber decorator:

```python hl_lines="3"
@broker.subscriber(
    "test",
    max_workers=4,
)
async def handle(msg): ...
```

With `max_workers=1` (the default) **FastStream** processes one message at a time per subscriber. With `max_workers=N`, up to `N` invocations of the handler can run at the same time.

This is handler-level parallelism and it is broker-agnostic: the setting means the same thing for RabbitMQ, Kafka, NATS, Redis and MQTT.

!!! note
    `max_workers` controls how many messages are processed **at once**, not how many are fetched from the broker. Those are separate concerns, and some brokers expose their own knob for the second one:

    - **RabbitMQ** — `prefetch_count` controls how many messages the broker may push before requiring an acknowledgement. See [Concurrency and Prefetch](../../rabbit/prefetch.md) for how the two settings interact.
    - **Kafka / Confluent** — the consumer pulls in batches; tune `max_poll_records` (and friends) to control how many records are fetched per poll. See [Kafka Subscriber concurrent processing](../../kafka/Subscriber/index.md#concurrent-processing) for the interaction with `AckPolicy`.
    - **NATS, Redis, MQTT** — only `max_workers` applies; there is no broker-side prefetch equivalent.
