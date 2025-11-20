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

The default behaviour, also implemented as such in the **FastStream**, uses the `#!python AckPolicy.ACK_FIRST` policy which automatically commits (*acks*) topic offset using [`enable.auto.commit`](https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit){.external-link target="_blank"} setting. This is the *at most once* consuming strategy.

However, if you wish to use *at least once* strategy, you should commit offset *AFTER* the message is processed correctly. To accomplish that, set a consumer group and use `#!python AckPolicy.ACK` strategy:

```python
@broker.subscriber(
    "test", group_id="group", ack_policy=AckPolicy.ACK
)
async def base_handler(body: str):
    ...
```

This way, the message processed will be acknowledged after handler execution. In the case of an exception being raised, the message will also be acknowledged.

If you want to retry on error, you can use `#!python AckPolicy.NACK_ON_ERROR` strategy. In this way offset will not be committed and consumer seeks to read this message again:

```python
@broker.subscriber(
    "test", group_id="group", ack_policy=AckPolicy.NACK_ON_ERROR
)
async def base_handler(body: str):
    ...
```

However, there are situations where you might want to use a different acknowledgement logic.

## Manual Acknowledgement

If you want to acknowledge a message manually, you can get direct access to the message object via the [Context](../getting-started/context.md#existing-fields){.internal-link} and acknowledge the message by calling the `ack` method:

```python
from faststream.kafka.annotations import KafkaMessage


@broker.subscriber(
    "test", group_id="group", ack_policy=AckPolicy.MANUAL
)
async def base_handler(body: str, msg: KafkaMessage):
    await msg.ack()
    # or
    await msg.nack()
    # or any custom logic
```

!!! tip
    You can use the `nack` method to prevent offset commit and the message can be consumed by another consumer within the same group.

**FastStream** will see that the message was already acknowledged and will do nothing at the end of the process.

## Summary **FastStream** [AckPolicy](../getting-started/acknowledgement.md){.internal-link} behavior

| _AckPolicy_     | On success    | On error      | Description                                                                                                                                                                 |
| --------------- | ------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MANUAL          | Do nothing    | Do nothing    | Consumer never commits offset, full manual control                                                                                                                          |
| ACK_FIRST       | Do nothing    | Do nothing    | Offset committed by Kafka client within [`enable.auto.commit`](https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit){.external-link target="_blank"} setting |
| ACK             | Commit offset | Commit offset |                                                                                                                                                                             |
| REJECT_ON_ERROR | Commit offset | Commit offset | Same as ack, because Kafka has not native support for rejecting messages                                                                                                    |
| NACK_ON_ERROR   | Commit offset | Seek offset   | Seek offset to read message again                                                                                                                                           |

## Interrupt Process

If you wish to interrupt the processing of a message at any call stack level and acknowledge the message, you can achieve that by raising the `faststream.exceptions.AckMessage`.

```python linenums="1" hl_lines="2 18"
{! docs_src/kafka/ack/errors.py !}
```

This way, **FastStream** interrupts the current message processing and acknowledges it immediately. Similarly, you can raise `NackMessage` as well to prevent the message from being committed.

{! includes/en/no_ack.md !}
