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

This object serves as a unified **FastStream** wrapper around the native broker library message (for example, [`confluent_kafka.Message`](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message) in the case of the *Confluent* Python library). The wrapper itself carries:

* `#!python body: bytes`
* `#!python headers: dict[str, str]`
* `#!python raw_headers: dict[str, bytes | None]`
* `#!python batch_raw_headers: list[dict[str, bytes | None]]`

and its `raw_message` (the `confluent_kafka.Message`) exposes the rest through methods, including:

* `#!python headers(): Sequence[Tuple[str, bytes]]`
* `#!python key(): Optional[Union[str, bytes]]`
* `#!python offset(): int`
* `#!python partition(): int`
* `#!python timestamp(): Tuple[int, int]`
* `#!python topic(): str`
* `#!python value(): Optional[Union[str, bytes]]`

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

## Binary Headers

Publishing methods, publishers, and `KafkaResponse` accept header values of type
`str`, `bytes`, or `None`. Strings are encoded as UTF-8; bytes and null values are
sent unchanged. For example, pass `headers={"event_id": b"\x00\xff"}` to
`broker.publish()` or `broker.publish_batch()`.

Use `message.raw_headers["event_id"]` to read the original bytes. Kafka does not
record whether a header was originally a string or bytes, so `raw_headers` also
contains bytes for headers published as strings.

The existing `message.headers` remains a decoded string view: invalid UTF-8 bytes
are replaced with `�`, and null values are omitted. Empty bytes become an empty
string. `raw_headers` retains invalid bytes, empty bytes, and null values without
loss. `raw_headers` keeps the last value when a header name occurs more than
once; use `message.raw_message.headers()` if you need duplicate entries.

For a batch, `raw_headers` contains the first message's headers and
`batch_raw_headers` contains one dictionary per message, in order. For a single
message, `batch_raw_headers` is empty. The dictionaries are snapshots: changing
them does not modify the native message.

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
