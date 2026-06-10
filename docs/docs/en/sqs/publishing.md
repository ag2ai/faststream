# Publishing

Publish to a queue with `broker.publish`:

```python linenums="1"
{! docs_src/sqs/publishing/publish.py [ln:16-21] !}
```

Headers are sent as SQS **MessageAttributes**. FastStream reserves the
`content-type`, `reply_to`, `correlation_id`, `empty-body`, and `base64-body`
attribute names for transport metadata.

!!! note "Binary payloads"
    SQS accepts only text message bodies. When you publish `bytes` that are not
    valid UTF-8 (an image, gzip, protobuf, ...), FastStream sends them
    base64-encoded and sets the reserved `base64-body` message attribute, so a
    FastStream subscriber receives the original bytes back transparently.
    Non-FastStream consumers of such messages should base64-decode the body when
    that attribute is present.

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
