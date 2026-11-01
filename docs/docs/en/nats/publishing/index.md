---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publishing

**FastStream** `NatsBroker` supports all regular [publishing use cases](../../getting-started/publishing/index.md){.internal-link}. You can use them without any changes.

However, if you wish to further customize the publishing logic, you should take a deeper look at specific `NatsBroker` parameters.

## NATS Publishing

Use `NatsBroker.publish()` to send messages directly.

```python
import asyncio
from faststream.nats import NatsBroker

async def pub():
    async with NatsBroker() as broker:
        await broker.publish(
            "Hi!",
            subject="test",
        )

asyncio.run(pub())
```

## Basic Arguments

The `publish` method accepts the following arguments:

* `#!python message = ""` - message to send.
* `#!python subject: str` - *subject* where the message will be sent.

## Message Parameters

* `#!python headers: dict[str, str] | None = None` - headers of the message being sent (used by consumers).
* `#!python reply_to: str = ""` - *subject* to send the response to.
* `#!python correlation_id: str | None = None` - message id, which helps to match the original message with the reply to it (generated automatically).

## NatsJS Parameters

* `#!python stream: str | JStream | None = None` - stream name or object used to validate that the subject is in the stream.
* `#!python timeout: float | None = None` - wait for the NATS server response.
* `#!python schedule: Optional[Schedule] = None` - Schedule to publish message at a specific time.

See the [JetStream example](../jetstream/index.md){.internal-link} for reusing the same `JStream` object in a subscriber and `broker.publish()`.
