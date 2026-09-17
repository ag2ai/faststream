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
    A record with a `None value()` is a Kafka tombstone, the delete marker on a compacted topic. `#!python msg.tombstone` is `#!python True` for it, keeping it distinct from an empty payload (`#!python b""`), and a `#!python None`-able body parameter of a **FastAPI** subscriber resolves to `#!python None` instead of failing validation.

    `#!python msg.body` is `#!python TOMBSTONE` for such a record: an empty `#!python bytes` subclass, so it still equals `#!python b""` and existing handlers are unaffected. `#!python isinstance(body, Tombstone)` is what tells a tombstone from an empty payload - and it is the only thing that does in a `#!python batch=True` subscriber, where each record is marked on its own and `#!python msg.tombstone` stays `#!python False` for the batch as a whole.

    ```python
    from faststream.confluent import TOMBSTONE, Tombstone

    await broker.publish(TOMBSTONE, "topic", key=b"user-1")
    ```

    An explicit `#!python TOMBSTONE` requires a key, since compaction deletes per key. It can also ride in a `#!python publish_batch()`, but not together with a custom `BatchCodecProto`, whose `#!python encode_batch()` has no way to express a null value for a single record.

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
You can use [Context Fields access](../getting-started/context.md#existing-fields) feature for this.

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
