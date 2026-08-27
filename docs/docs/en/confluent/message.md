---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Access to Message Information

As you may know, **FastStream** serializes a message body and provides you access to it through function arguments. However, there are times when you need to access additional message attributes such as offsets, headers, or other metadata.

## Message Access

You can easily access this information by referring to the message object in the [Context](../getting-started/context.md#existing-fields)

This object serves as a unified **FastStream** wrapper around the native broker library message (for example, [`confluent_kafka.Message`](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message) in this case which uses *Confluent* python library). It contains most of the required information, including:

* `#!python headers(): Sequence[Tuple[str, bytes]]`
* `#!python key(): Optional[Union[str, bytes]]`
* `#!python offset(): int`
* `#!python partition(): int`
* `#!python timestamp(): Tuple[int, int]`
* `#!python topic(): str`
* `#!python value(): Optional[Union[str, bytes]]`

!!! note
    A record with a `None value()` is a Kafka tombstone, the delete marker on a compacted topic. `#!python msg.body` is `#!python faststream.message.TOMBSTONE` - a `#!python bytes` subclass equal to `#!python b""`, so a handler that doesn't care sees an empty body exactly as before, while `#!python isinstance(msg.body, Tombstone)` tells the two apart. For a single record `#!python msg.tombstone` says the same thing; for a batch, check each element of `#!python msg.body`.

    Publish one with `#!python await broker.publish(TOMBSTONE, "topic", key=b"...")`. An explicit `#!python TOMBSTONE` requires a key, since compaction deletes per key and a keyless one deletes nothing. Passing `#!python None` as the body does the same, with or without a key, but is deprecated; it will encode normally in 0.8.

    Batching a tombstone alongside a custom `#!python BatchCodecProto` raises, since `#!python encode_batch()` has no way to express a null value for one record of the batch.

For example, if you would like to access the headers of an incoming message, you would do so like this:

```python hl_lines="1 6"
from faststream.confluent import KafkaMessage

@broker.subscriber("test")
async def base_handler(
    body: str,
    msg: KafkaMessage,
):
    print(msg.headers)
```

## Raw Message Access

In some cases, you may want to access the raw `confluent_kafka.Message` created by `confluent_kafka` library. In such cases, you can do so by:

```python hl_lines="1 6"
from faststream.confluent import KafkaMessage

@broker.subscriber("test")
async def base_handler(
    body: str,
    msg: KafkaMessage,
):
    print(msg.raw_message.headers())
```

## Message Fields Access

In most cases, you don't need all message fields; you need to know just a part of them.
You can use [Context Fields access](../getting-started/context.md#existing_fields) feature for this.

For example, you can get access to the `headers` like this:

```python hl_lines="6"
from faststream import Context

@broker.subscriber("test")
async def base_handler(
    body: str,
    headers: str = Context("message.headers"),
):
    print(headers)
```

{! includes/message/headers.md !}
