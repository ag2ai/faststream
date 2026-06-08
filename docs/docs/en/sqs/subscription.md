# Subscription

Each subscriber long-polls its queue in a background task and dispatches every
received message to your handler:

```python linenums="1"
{! docs_src/sqs/subscription/basic.py [ln:10-16] !}
```

| Parameter | Description |
|-----------|-------------|
| `wait_time_seconds` | SQS `WaitTimeSeconds` for long polling. Higher values reduce empty receives and request cost. |
| `max_messages` | SQS `MaxNumberOfMessages` per receive. |
| `visibility_timeout` | How long a received message stays invisible to other consumers while being processed. |

## Accessing message metadata

```python linenums="1"
{! docs_src/sqs/subscription/message_info.py [ln:5,11-13] !}
```

## Declaring queues on subscribe

Pass an `SQSQueue`/`FifoQueue` to create the queue automatically on startup:

```python linenums="1"
{! docs_src/sqs/subscription/declared_queue.py [ln:4,10-11] !}
```
