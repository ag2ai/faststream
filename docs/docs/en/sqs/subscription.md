# Subscription

Each subscriber long-polls its queue in a background task and dispatches every
received message to your handler:

```python linenums="1"
@broker.subscriber(
    "my-queue",
    wait_time_seconds=10,   # long-poll wait (0-20)
    max_messages=10,        # messages per receive (1-10)
    visibility_timeout=30,  # per-receive visibility override
)
async def handler(msg: str) -> None:
    ...
```

| Parameter | Description |
|-----------|-------------|
| `wait_time_seconds` | SQS `WaitTimeSeconds` for long polling. Higher values reduce empty receives and request cost. |
| `max_messages` | SQS `MaxNumberOfMessages` per receive. |
| `visibility_timeout` | How long a received message stays invisible to other consumers while being processed. |

## Accessing message metadata

```python linenums="1"
from faststream.sqs.annotations import SQSMessage


@broker.subscriber("my-queue")
async def handler(body: str, msg: SQSMessage) -> None:
    print(msg.message_id, msg.headers, msg.correlation_id)
```

## Declaring queues on subscribe

Pass an `SQSQueue`/`FifoQueue` to create the queue automatically on startup:

```python linenums="1"
from faststream.sqs import SQSQueue


@broker.subscriber(SQSQueue(name="orders", visibility_timeout=60))
async def handler(msg: str) -> None:
    ...
```
