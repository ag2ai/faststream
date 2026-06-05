# Publishing

Publish to a queue with `broker.publish`:

```python linenums="1"
await broker.publish(
    "payload",
    "my-queue",
    headers={"trace-id": "abc"},
    delay_seconds=5,
)
```

Headers are sent as SQS **MessageAttributes**. FastStream reserves the
`content-type`, `reply_to`, and `correlation_id` attribute names for transport
metadata.

## Declared queues

Pass an `SQSQueue` instead of a name to have FastStream create the queue (with
the given attributes) on startup:

```python linenums="1"
from faststream.sqs import SQSQueue

queue = SQSQueue(name="orders", visibility_timeout=60, message_retention_period=86400)
await broker.publish("data", queue)
```

## Publisher objects

Register a reusable publisher with `@broker.publisher`:

```python linenums="1"
publisher = broker.publisher("out-queue", headers={"source": "svc-a"})


@publisher
@broker.subscriber("in-queue")
async def handler(msg: str) -> str:
    return msg.upper()  # return value is published to "out-queue"
```

## Batch publishing

```python linenums="1"
await broker.publish_batch("a", "b", "c", queue="my-queue")
```

A single `SendMessageBatch` request carries up to 10 messages.
