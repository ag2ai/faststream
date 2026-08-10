---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Concurrency and Prefetch

RabbitMQ subscribers have two settings that look like they do the same thing but do not: [`max_workers`](../getting-started/subscription/concurrency.md) and `prefetch_count`. Mixing them up is a common source of confusion, so it helps to keep their roles straight.

## `prefetch_count`

`prefetch_count` comes from AMQP's `basic.qos`. It tells the broker how many unacknowledged messages it may have outstanding on a given channel at any time.

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

A useful rule of thumb: set `prefetch_count` at least as large as `max_workers`, otherwise extra workers sit idle waiting for messages.

## The common confusion

> Does `prefetch_count=10` mean ten messages are processed at the same time?

No. It means the **broker** may deliver up to ten messages before it expects acknowledgments. How many are *processed in parallel* is determined by `max_workers`. With the default `max_workers=1`, the ten prefetched messages are processed one after another — `prefetch_count` only changes how aggressively they are pulled from the broker.
