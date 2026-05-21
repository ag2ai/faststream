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

**FastStream** gives you two independent levers for processing messages concurrently. Mixing them up is a common source of confusion, so it helps to keep their roles straight.

## `max_workers` — handler-level parallelism

`max_workers` is the **number of concurrent handler invocations** a single subscriber may run. It is available on every broker's subscriber decorator:

```python hl_lines="3"
@broker.subscriber(
    "test",
    max_workers=4,
)
async def handle(msg): ...
```

With `max_workers=1` (the default) **FastStream** processes one message at a time per subscriber. With `max_workers=N`, up to `N` invocations of the handler can run at the same time.

## `prefetch_count` — RabbitMQ flow control

`prefetch_count` is **RabbitMQ-specific**. It comes from AMQP's `basic.qos` and tells the broker how many unacknowledged messages it may have outstanding on a given channel at any time.

It is set at the channel level:

```python hl_lines="6"
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel

broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    channel=Channel(prefetch_count=10),
)
```

`prefetch_count=N` does **not** mean "process `N` messages at the same time". It means "let the broker push up to `N` messages to this consumer before requiring an ack". It is a buffer / flow-control window — not parallelism.

## How they combine

The two settings are orthogonal:

| `max_workers` | `prefetch_count` | Behavior                                                                                |
|---------------|------------------|-----------------------------------------------------------------------------------------|
| `1`           | `1`              | One message in flight, processed sequentially. The classic per-message round trip.       |
| `1`           | `N`              | Up to `N` messages held locally by the consumer, but **still processed one at a time**.  |
| `K`           | `N` (≥ `K`)      | Up to `K` invocations of the handler run concurrently; up to `N` may be buffered.        |
| `K`           | `< K`            | Concurrency is throttled by `prefetch_count`: the broker will not push enough messages to keep all workers busy. |

A useful rule of thumb for RabbitMQ subscribers: set `prefetch_count` at least as large as `max_workers`, otherwise extra workers sit idle waiting for messages.

## Other brokers

- **Kafka / Confluent.** There is no `prefetch_count`. The consumer pulls in batches; tune `max_poll_records` (and friends) to control how many records are fetched per poll, and use `max_workers` for handler parallelism. See [Kafka Subscriber concurrent processing](../../kafka/Subscriber/index.md#concurrent-processing) for the interaction with `AckPolicy`.
- **Redis, NATS, MQTT.** Only `max_workers` applies. There is no broker-side prefetch knob equivalent to RabbitMQ's.

## The common confusion

> Does `prefetch_count=10` mean ten messages are processed at the same time?

No. It means the **broker** may deliver up to ten messages before it expects acknowledgments. How many are *processed in parallel* is determined by `max_workers`. With the default `max_workers=1`, the ten prefetched messages are processed one after another — `prefetch_count` only changes how aggressively they are pulled from the broker.
