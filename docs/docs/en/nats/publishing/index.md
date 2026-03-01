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

`NatsBroker` also uses the unified `publish` method (from a `publisher` object) to send messages.

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
* `#!python correlation_id: str | None = None` - message id, which helps to match the original message with the reply to it (generated automatically).

## NatsJS Parameters

* `#!python stream: str | None = None` - validate that the subject is in the stream.
* `#!python timeout: float | None = None` - wait for the NATS server response.
* `#!python schedule: Optional[Schedule] = None` - Schedule to publish message at a specific time.
