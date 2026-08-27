---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Accessing Redis Message Information with FastStream

In **FastStream**, messages passed through a **Redis** broker are serialized and can be interacted with just like function parameters. However, you might occasionally need to access more than just the message content, such as metadata and other attributes.

## Redis Message Access

When dealing with **Redis** broker in **FastStream**, you can easily access message details by using the `RedisMessage` object which wraps the underlying message with additional context information. This object is specifically tailored for **Redis** and contains relevant message attributes:

* `#!python body: Union[bytes, Any]`
* `#!python raw_message: Any`
* `#!python decoded_body: Optional[DecodedMessage]`
* `#!python headers: dict[str, Any]`
* `#!python path: dict[str, Any]`
* `#!python content_type: Optional[str]`
* `#!python reply_to: str`
* `#!python message_id: str`
* `#!python correlation_id: str`
* `#!python processed: bool`
* `#!python committed: bool`

For instance, if you need to retrieve headers from an incoming **Redis** message, here’s how you might do it:

```python
from faststream.redis import RedisMessage

@broker.subscriber("test-stream")
async def stream_handler(msg: str, message: RedisMessage):
    print(message.headers)
```

## Targeted Message Fields Access

It's common to require only specific elements of the message rather than the entire data structure. For this purpose, FastStream allows you to access individual message fields by specifying the field you are interested in as an argument in your handler function.

For example, if you want to access the headers directly, you might do it as follows:

```python
from faststream import Context

@broker.subscriber("test-stream")
async def stream_handler(
    msg: str,
    headers: dict[str, Any] = Context("message.headers"),
):
    print(headers)
```

The `Context` object lets you reference message attributes directly, making your handler functions neater and reducing the amount of boilerplate code needed.

## Redis Stream Delivery Count

For a single stream message consumed through a consumer group, call
`await message.get_delivery_count()` on `RedisStreamMessage` to read its current
delivery count from the Redis [Pending Entries List](https://redis.io/docs/latest/develop/data-types/streams/#viewing-pending-messages){.external-link target="_blank"}.

```python
from faststream.redis import RedisStreamMessage, StreamSub

@broker.subscriber(
    stream=StreamSub("orders", group="workers", consumer="worker-1")
)
async def handle_order(message: RedisStreamMessage) -> None:
    delivery_count = await message.get_delivery_count()
```

Each call performs an exact-ID `XPENDING RANGE` query, so the value is a live
snapshot rather than cached message metadata. A newly delivered message returns
`1`, and `XAUTOCLAIM` increments the count before the claimed message reaches
the handler. If the message is not associated with a consumer group, has no
Redis message ID, or is no longer pending after acknowledgment, the method
returns `1`. Redis errors such as a missing consumer group are propagated.

This method is available on `RedisStreamMessage`, not on batched stream
messages. It adds one Redis round trip per call.
