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

RabbitMQ subscribers are concurrent by default. A delivery is handled as it arrives, so several invocations of your handler can be in flight at the same time — you do not opt into that, and there is no `max_workers` option here.

What you tune instead is the opposite: **how many messages the broker is allowed to push at you**. Without a limit it will keep sending as fast as it can, which is rarely what you want.

## `prefetch_count`

`prefetch_count` comes from AMQP's `basic.qos`. It tells the broker how many unacknowledged messages it may have outstanding on a channel at any time.

It is set at the channel level:

```python hl_lines="6"
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel

broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    default_channel=Channel(prefetch_count=10),
)
```

With `prefetch_count=10`, the broker will deliver at most ten messages that have not yet been acknowledged. As each one is acked, room opens up for the next.

You can also give a single subscriber its own channel, so its limit is independent of the rest of the application:

```python hl_lines="3"
@broker.subscriber(
    "heavy-queue",
    channel=Channel(prefetch_count=1),
)
async def handle(msg): ...
```

## Why you want a limit

Leaving `prefetch_count` unset means the broker keeps pushing. For a fast producer and a slow handler that has two costs:

- **Memory.** Undelivered work piles up in your process rather than staying in the queue.
- **Distribution.** Messages already sitting in one consumer's buffer cannot be picked up by another consumer, so adding replicas stops helping.

Setting `prefetch_count` to a small number keeps the backlog in RabbitMQ, where it can still be redistributed.

## Choosing a value

There is no universal answer, but the shape of the trade-off is consistent:

| `prefetch_count` | Behavior |
|---|---|
| `1` | One unacknowledged message at a time. Slowest throughput, but work spreads evenly across consumers — a good fit for long or uneven tasks. |
| small (e.g. `10`) | Enough of a buffer to hide network latency while keeping the bulk of the backlog in the queue. A reasonable default. |
| large / unset | Highest throughput for short, uniform tasks, at the cost of memory and of one consumer hoarding messages. |

If handling a message is slow or its duration varies a lot, prefer a small value. If messages are tiny and uniform, a larger window is usually fine.

## `global_qos`

By default the limit applies to each consumer on the channel separately. `Channel(global_qos=True)` shares one limit across every subscriber using that channel instead:

```python
broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    default_channel=Channel(prefetch_count=10, global_qos=True),
)
```

See the RabbitMQ documentation on [consumer prefetch](https://www.rabbitmq.com/docs/consumer-prefetch){.external-link target="_blank"} for the full semantics.
