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

## Concurrent processing

By default messages from one poll are processed sequentially. Set `max_workers`
to handle up to that many messages at the same time:

```python linenums="1"
{! docs_src/sqs/subscription/concurrency.py [ln:10-11] !}
```

!!! warning
    `max_workers` can't be combined with `batch=True` or FIFO queues —
    concurrent processing would break message-group ordering.

## Long-running handlers

If a handler runs longer than the queue's `VisibilityTimeout`, SQS redelivers
the message while the first consumer is still working. Enable
`extend_visibility` to keep extending the timeout with a background heartbeat
until the handler finishes:

```python linenums="1"
{! docs_src/sqs/subscription/long_running.py [ln:10-15] !}
```

!!! note
    `extend_visibility=True` requires an explicit `visibility_timeout` — the
    heartbeat re-extends the message by that value every `visibility_timeout / 2`
    seconds.
