---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Atomic Batch Publishing

NATS server 2.12 adds atomic batch publishing to JetStream. A stream stores all the messages of a batch, or none of them.

Use it when a group of messages must arrive together. For example, an order and its items.

## Enable Atomic Publishing on a Stream

Set `allow_atomic=True` on the stream. A stream without this option rejects each batch.

```python
from faststream.nats import JStream

stream = JStream("orders", allow_atomic=True)
```

## Publish a Batch

Call `broker.publish_batch` with the message bodies:

```python linenums="1" hl_lines="7 17-23"
{! docs_src/nats/js/atomic_batch.py !}
```

`publish_batch` sends the messages in order. The last message commits the batch. The method returns the `PubAck` of the last message.

## Errors

If the server rejects the batch, `publish_batch` raises an error, and the stream stores no message of the batch. These conditions cause an error:

- The stream does not have `allow_atomic=True`.
- The server version is older than 2.12.
- The batch is larger than the server limit. The default limit is 1000 messages.
- The `batch_id` is longer than 64 characters.

## Key Points

- All messages of a batch have the same `subject`, `headers`, and `correlation_id`.
- Use the `batch_id` option to set the batch identifier. The server accepts 64 characters or less. If you do not set it, the broker uses its `id_generator`.
- `publish_batch` always uses JetStream. Use the `stream` option to make sure that the subject is in the correct stream.
- `TestNatsBroker` sends each message of the batch to the subscribers. It does not simulate an error.
