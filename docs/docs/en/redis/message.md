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
