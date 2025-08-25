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

As you may know, *Kafka* consumer should commit a topic offset when consuming a message.

The default behaviour, also implemented as such in the **FastStream**, automatically commits (*acks*) topic offset on message consumption. This is the *at most once* consuming strategy.

However, if you wish to use *at least once* strategy, you should commit offset *AFTER* the message is processed correctly. To accomplish that, set a consumer group and disable `auto_commit` option like this:

```python
@broker.subscriber(
    "test", group_id="group", auto_commit=False
)
async def base_handler(body: str):
    ...
```

This way, upon successful return of the processing function, the message processed will be acknowledged. In the case of an exception being raised, the message will not be acknowledged.

However, there are situations where you might want to use a different acknowledgement logic.

## Manual Acknowledgement

If you want to acknowledge a message manually, you can get direct access to the message object via the [Context](../getting-started/context.md#existing-fields){.internal-link} and acknowledge the message by calling the `ack` method:

```python
from faststream.confluent.annotations import KafkaMessage


@broker.subscriber(
    "test", group_id="group", auto_commit=False
)
async def base_handler(body: str, msg: KafkaMessage):
    await msg.ack()
    # or
    await msg.nack()
```

!!! tip
    You can use the `nack` method to prevent offset commit and the message can be consumed by another consumer within the same group.

**FastStream** will see that the message was already acknowledged and will do nothing at the end of the process.

## Interrupt Process

If you wish to interrupt the processing of a message at any call stack level and acknowledge the message, you can achieve that by raising the `faststream.exceptions.AckMessage`.

```python linenums="1" hl_lines="2 18"
{! docs_src/confluent/ack/errors.py !}
```

This way, **FastStream** interrupts the current message processing and acknowledges it immediately. Similarly, you can raise `NackMessage` as well to prevent the message from being committed.

{! includes/en/no_ack.md !}
