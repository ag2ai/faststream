---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Consuming acknowledgements

MQTT delivery acknowledgement is tied to **QoS** at the protocol level:

- **QoS 0** — fire-and-forget; there is no PUBACK/PUBREC to send.
- **QoS 1** — broker expects a **PUBACK** after the client accepts the message.
- **QoS 2** — full four-step handshake ending in **PUBCOMP**.

FastStream configures the underlying zmqtt subscription with `auto_ack` according to your [`AckPolicy`](../getting-started/acknowledgement.md){.internal-link}.

## Default behaviour (`AckPolicy.ACK_FIRST`)

For **QoS 1 and 2**, the default policy is **`ACK_FIRST`**: zmqtt acknowledges the message **before** your handler runs (`auto_ack=True`). That matches “process after the broker has been told we took responsibility,” and is similar in spirit to Kafka’s auto-commit-before-process trade-off.

## After processing (`AckPolicy.ACK`)

Use `ack_policy=AckPolicy.ACK` so the message is acknowledged **after** successful processing (zmqtt `auto_ack=False`, FastStream calls `MQTTMessage.ack()` which forwards to `zmqtt.Message.ack()`).

## Manual (`AckPolicy.MANUAL`)

Call `await msg.ack()` yourself when processing is done. `MQTTMessage.nack()` and `reject()` are implemented as **`ack()`** on MQTT — the protocol does not offer Rabbit-style requeue via nack; use QoS and broker features for redelivery semantics.

## Policies with limited MQTT support

`NACK_ON_ERROR` and `REJECT_ON_ERROR` are **not** mapped to distinct MQTT broker behaviours. FastStream emits a **runtime warning** and will still complete processing; treat these policies as unsupported for strict MQTT semantics.

## Interrupting with exceptions

You can raise `AckMessage` or `NackMessage` from `faststream.exceptions` to short-circuit processing, consistent with other brokers ([acknowledgement docs](../getting-started/acknowledgement.md){.internal-link}).

```python
from faststream import AckPolicy
from faststream.mqtt import MQTTBroker, MQTTMessage, QoS

broker = MQTTBroker(version="5.0")


@broker.subscriber("jobs/run", qos=QoS.AT_LEAST_ONCE, ack_policy=AckPolicy.MANUAL)
async def work(payload: dict, msg: MQTTMessage) -> None:
    try:
        ...
    finally:
        await msg.ack()
```
