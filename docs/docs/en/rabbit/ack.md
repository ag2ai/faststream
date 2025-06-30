---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Consuming Acknowledgements

As you may know, *RabbitMQ* employs a rather extensive [Acknowledgement](https://www.rabbitmq.com/confirms.html){.external-link target="_blank"} policy.

In most cases, **FastStream** automatically acknowledges (*acks*) messages on your behalf. When your function executes correctly, including sending all responses, a message will be acknowledged (and rejected in case of an exception).

However, there are situations where you might want to use a different acknowledgement logic.

## Manual acknowledgement

If you want to acknowledge a message manually, you can get access directly to the message object via the [Context](../getting-started/context/existed.md){.internal-link} and call the method.

```python
from faststream.rabbit.annotations import RabbitMessage

@broker.subscriber("test")
async def base_handler(body: str, msg: RabbitMessage):
    await msg.ack()
    # or
    await msg.nack()
    # or
    await msg.reject()
```

**FastStream** will see that the message was already acknowledged and will do nothing at process end.

## Interrupt Process

If you want to interrupt message processing at any call stack, you can raise `faststream.exceptions.AckMessage`

```python linenums="1" hl_lines="2 16"
{! docs_src/rabbit/ack/errors.py !}
```

This way, **FastStream** interrupts the current message processing and acknowledges it immediately. Also, you can raise `NackMessage` and `RejectMessage` too.

{! includes/en/no_ack.md !}
