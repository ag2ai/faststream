---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Stream Acknowledgement

When working with *Redis* streams in the **FastStream** library, it's important to manage message acknowledgements carefully to ensure that messages are not lost and that they have been processed as intended.

By default, when using the **FastStream** with a Redis stream, the library will automatically acknowledge (*ack*) that a message has been processed. This follows the *at most once* processing guarantee.

## Manual Acknowledgement

In cases where you want explicit control over when a message is acknowledged, you can manually acknowledge a message by accessing the `ack` and `nack` methods provided:

```python
from faststream.redis.annotations import RedisMessage, Redis

# Setup broker and faststream app
...

@broker.subscriber(StreamSub("test-stream", group="test-group", consumer="1"))
async def base_handler(body: dict, msg: RedisMessage, redis: Redis):
    # Process the message
    ...

    # Manually acknowledge the message
    await msg.ack(redis)
    # or, if processing fails and you want to reprocess later
    await msg.nack()
```

Using `ack` will mark the message as processed in the stream, while `nack` is useful for situations where you might need to reprocess a message due to a handling failure.

## Interrupt Process

If the need arises to instantly interrupt message processing at any point in the call stack and acknowledge the message, you can achieve this by raising the `faststream.exceptions.AckMessage` exception:

```python linenums="1" hl_lines="2 16"
{! docs_src/redis/stream/ack_errors.py !}
```

By raising `AckMessage`, **FastStream** will halt the current message processing routine and immediately acknowledge it. Analogously, raising `NackMessage` would prevent the message from being acknowledged and could lead to its subsequent reprocessing by the same or a different consumer.

{! includes/en/no_ack.md !}
