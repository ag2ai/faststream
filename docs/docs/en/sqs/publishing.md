# Publishing

Publish to a queue with `broker.publish`:

```python linenums="1"
{! docs_src/sqs/publishing/publish.py [ln:16-21] !}
```

Headers are sent as SQS **MessageAttributes**. FastStream reserves the
`content-type`, `reply_to`, and `correlation_id` attribute names for transport
metadata.

## Declared queues

Pass an `SQSQueue` instead of a name to have FastStream create the queue (with
the given attributes) on startup:

```python linenums="1"
{! docs_src/sqs/publishing/declared_queue.py [ln:4,9,18] !}
```

## Publisher objects

Register a reusable publisher with `@broker.publisher`:

```python linenums="1"
{! docs_src/sqs/publishing/publisher_object.py [ln:9,12-15] !}
```

## Batch publishing

```python linenums="1"
{! docs_src/sqs/publishing/batch.py [ln:16] !}
```

A single `SendMessageBatch` request carries up to 10 messages.
