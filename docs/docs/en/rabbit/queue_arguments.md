---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Queue Arguments

Beyond `durable`, `exclusive` and `auto_delete`, RabbitMQ configures a queue through **optional arguments**: a dictionary of `x-*` keys sent with the declaration. They control message TTL, queue length, overflow behavior, dead-lettering, priorities and more. The full list lives in the [RabbitMQ documentation](https://www.rabbitmq.com/docs/queues#optional-arguments){.external-link target="_blank"}.

`RabbitQueue` forwards them through the `arguments` parameter:

```python
from faststream.rabbit import RabbitQueue

queue = RabbitQueue(
    "orders",
    arguments={
        "x-message-ttl": 60_000,
        "x-max-length": 10_000,
        "x-overflow": "reject-publish",
    },
)
```

`x-queue-type` is set for you from the `queue_type` parameter, so don't put it into `arguments`. The accepted keys depend on that type: `RabbitQueue.__init__` is overloaded by `queue_type`, so **mypy** rejects a key the chosen type doesn't support and your editor can complete the rest.

## Common Arguments

| Argument                    | Queue types      | Meaning                                                                             |
| --------------------------- | ---------------- | ----------------------------------------------------------------------------------- |
| `x-message-ttl`             | classic, quorum  | Milliseconds a message may wait in the queue before it expires.                     |
| `x-expires`                 | classic, quorum  | Milliseconds of no use (no consumers, no declarations) after which the queue is deleted. |
| `x-max-length`              | classic, quorum  | Maximum number of ready messages.                                                   |
| `x-max-length-bytes`        | all              | Maximum total body size of ready messages.                                          |
| `x-overflow`                | classic, quorum  | What to do at the limit: `drop-head` (default), `reject-publish` or `reject-publish-dlx`. |
| `x-single-active-consumer`  | classic, quorum  | Deliver to one consumer at a time, failing over to the next one.                    |
| `x-dead-letter-exchange`    | classic, quorum  | Exchange receiving rejected and expired messages. See [below](#dead-letter-queue).  |
| `x-dead-letter-routing-key` | classic, quorum  | Routing key to dead-letter with. Keeps the original key when unset.                 |
| `x-max-priority`            | classic          | Enables [message priorities](https://www.rabbitmq.com/docs/priority){.external-link target="_blank"} up to this value. |
| `x-delivery-limit`          | quorum           | Redeliveries after which a message is dropped or dead-lettered.                     |
| `x-max-age`                 | stream           | Retention of a [stream](./examples/stream.md){.internal-link}, for example `#!python "7D"`. |

Arguments that belong to the **consumer** rather than the queue (`x-stream-offset`, `x-priority`) go to the subscriber's `consume_args` instead, as the [stream example](./examples/stream.md){.internal-link} shows.

!!! warning "Queue arguments are immutable"
    RabbitMQ refuses to redeclare an existing queue with different arguments and fails the declaration with `PRECONDITION_FAILED`. To change the arguments of a queue that already exists on the server, delete that queue first or pick a new name.

## Dead Letter Queue

When a consumer rejects a message, or a message sits in a queue longer than its TTL, RabbitMQ can forward it to a **dead letter exchange** instead of dropping it. A queue bound to that exchange collects those messages, so you can inspect, alert on, or replay them later.

The [dead-lettering](https://www.rabbitmq.com/docs/dlx){.external-link target="_blank"} rules live on the **source queue** as the `x-dead-letter-exchange` and `x-dead-letter-routing-key` arguments. The dead letter exchange and the queue bound to it are ordinary objects that you declare like any other:

```python linenums="1" hl_lines="14-15 17-24 27 30 35 41"
{! docs_src/rabbit/dead_letter.py !}
```

Here is what happens at startup and on each message:

1. **FastStream** declares the `orders-dlx` exchange and binds the `orders-dead` queue to it with the routing key `orders`, because the second subscriber refers to both.
2. It declares the `orders` queue with the dead-letter arguments. All subscribers are declared before any message is consumed, so the order of the handlers in the file doesn't matter.
3. `handle_order` raises `RejectMessage` for a bad order. **FastStream** rejects the message without requeue, RabbitMQ publishes it to `orders-dlx` with the routing key `orders`, and it lands in `orders-dead`.
4. `handle_dead_letter` receives the very same message. RabbitMQ adds an `x-death` header describing why it was dead-lettered (`rejected`, `expired`, `maxlen` or `delivery_limit`) and from which queue.

The same path is taken by any message that expires after `x-message-ttl` milliseconds, and by messages a handler fails on with an unhandled exception, since the default [acknowledgement policy](./ack.md){.internal-link} rejects those too.

!!! tip
    Without `x-dead-letter-routing-key`, a message is dead-lettered with the routing key it was originally published with. That is handy when one dead letter exchange serves several queues: bind one dead letter queue per source routing key, or use a `fanout` exchange to collect everything in one place.
