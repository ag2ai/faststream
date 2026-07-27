# FIFO queues

FIFO queues guarantee ordering and exactly-once processing within a message
group. Declare one with `FifoQueue` — the `.fifo` suffix is added automatically:

```python linenums="1"
{! docs_src/sqs/fifo/app.py [ln:3-18] !}
```

## Publishing to FIFO queues

FIFO sends require a `MessageGroupId`. When content-based deduplication is off,
also provide a `deduplication_id`:

```python linenums="1"
{! docs_src/sqs/fifo/app.py [ln:22-27] !}
```

Messages sharing a `group_id` are delivered in strict order; different groups
are processed in parallel.

## Receive-retry deduplication (`request_attempt_id`)

For FIFO queues you can pass a `request_attempt_id` to the subscriber. It maps to
the SQS `ReceiveRequestAttemptId` — the deduplication token SQS uses when a
`ReceiveMessage` call fails (e.g. a network error): retrying with the same token
returns the *same* batch of messages instead of leaving them invisible.

```python linenums="1"
{! docs_src/sqs/fifo/request_attempt_id.py [ln:3-13] !}
```

This parameter is FIFO-only — setting it on a standard (non-`.fifo`) queue raises
a `SetupError` at startup.
