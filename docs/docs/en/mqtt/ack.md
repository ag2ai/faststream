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

## Default behaviour (`AckPolicy.ACK`)

The default policy is **`ACK`**: the message is acknowledged **after** your handler runs, regardless of outcome.

- **Success** — `ack()` is called; for QoS 1/2 the PUBACK is sent.
- **Error** — `ack()` is still called; the message is acknowledged and **will not be redelivered**.

This is the right default for most MQTT workloads: messages are not acked until processing is attempted, errors do not cause redelivery, and processing status is tracked correctly by middlewares such as Prometheus.

## Ack before processing (`AckPolicy.ACK_FIRST`)

Use `ack_policy=AckPolicy.ACK_FIRST` to acknowledge the message **before** your handler runs (`auto_ack=True`). Useful when you want to release the broker's tracking immediately and can tolerate losing the message if the handler crashes.

## Reject on error (`AckPolicy.REJECT_ON_ERROR`)

On success `ack()` is called; on error `reject()` is called. Because MQTT has no reject primitive, `MQTTMessage.reject()` maps to `ack()`, so in practice this behaves identically to `AckPolicy.ACK` on MQTT.

## Manual (`AckPolicy.MANUAL`)

Call `await msg.ack()` yourself when processing is done.

`MQTTMessage.nack()` is a **no-op** — the protocol has no nack primitive. With `auto_ack=False` (i.e. any policy other than `ACK_FIRST`) the zmqtt client will not send a PUBACK for QoS 1/2 messages until you call `ack()` explicitly, so leaving a message un-acked causes the broker to redeliver it.

`MQTTMessage.reject()` calls `ack()` — MQTT offers no reject semantic, so FastStream acknowledges the message to prevent redelivery.

## `NACK_ON_ERROR`

`NACK_ON_ERROR` is supported but behaves differently from other brokers: `MQTTMessage.nack()` is a no-op, so on error **no PUBACK is sent**. For QoS 1/2 topics the broker will redeliver the message. FastStream emits a **runtime warning** when this policy is used, because the redelivery behaviour may be surprising.

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
