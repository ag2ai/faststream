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
