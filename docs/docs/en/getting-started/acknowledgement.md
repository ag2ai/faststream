# Acknowledgment

Due to the possibility of unexpected errors during message processing, FastStream provides an `ack_policy` parameter that allows users to control how messages are handled. This parameter determines when and how messages should be acknowledged or rejected based on the result of the message processing.

## AckPolicy

`AckPolicy` is an enumerated type (`Enum`) in FastStream that specifies the message acknowledgment strategy. It determines how the system responds after receiving and processing a message.

### Availability

`AckPolicy` is supported by the following brokers:

- [**Kafka**](../kafka/index.md){.internal-link}
- [**RabbitMQ**](../rabbit/index.md){.internal-link}
- [**NATS JetStream**](../nats/jetstream/index.md){.internal-link}
- [**Redis Streams**](../redis/streams/index.md){.internal-link}

### Usage

You must specify the `ack_policy` parameter when creating a subscriber:

```python linenums="1" hl_lines="9"
from faststream import FastStream, Logger, AckPolicy
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream(broker)

@broker.subscriber(
    "test",
    ack_policy=AckPolicy.REJECT_ON_ERROR,
)
async def handler(msg: str, logger: Logger) -> None:
    logger.info(msg)
```

### Available Options

Each `AckPolicy` variant includes behavior examples for both successful processing and error scenarios. Note that broker-specific behaviors are also included.

| Policy                      | Description                                                                                                                             | On Success                                                                   | On Error                                              | Broker Notes                                                             |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------ |
| `ACK_FIRST`       | Acknowledge immediately upon receipt, before processing begins.                                                                         | Message is acknowledged early;<br/>may be lost if processing fails.              | Acknowledged despite error;<br/>message not re-delivered. | Kafka commits offset;<br/>NATS, Redis, and RabbitMQ acknowledge immediately. |
| `ACK`             | Acknowledge only after processing completes, regardless of success.                                                                     | Ack after success.                                          | Ack sent anyway;<br/>message not redelivered.             | Kafka: offset commit; others: explicit ack.                              |
| `REJECT_ON_ERROR` | Reject message if an unhandled exception occurs, permanently discarding it;<br/>otherwise, ack.                                             | Ack after success. | Message discarded; no retry.                          | RabbitMQ/NATS drops message. Kafka commits offset.                       |
| `NACK_ON_ERROR`   | Nack on error to allow message redelivery, ack after success otherwise.                                                                 | Ack after success.                                                           | Redeliver; attempt to resend message.                 | Redis Streams and RabbitMQ redelivers; Kafka commits as fallback.        |
| `MANUAL`      | No automatic acknowledgement. User must manually handle the completion via message methods<ul><li> `#!python msg.ack()`</li><li>`#!python msg.nack()`<li>`#!python msg.reject()`</li></ul> | | | |

---

### When to Use

- Use `ACK_FIRST` for scenarios with high throughput where some message loss can be acceptable.
- Use `ACK` if you want the message to be acknowledged, regardless of success or failure.
- Use `REJECT_ON_ERROR` to permanently discard messages on failure.
- Use `NACK_ON_ERROR` to retry messages in case of failure.
- Use `MANUAL` to fully manually control message acknowledgment (for example, calling `#!python message.ack()` yourself).

---

### Extended Examples

#### Automatic Retry on Failure

```python linenums="1" hl_lines="7 10"
from faststream import FastStream, AckPolicy, Logger
from faststream.rabbitmq import RabbitBroker

broker = RabbitBroker()
app = FastStream(broker)

@broker.subscriber("orders", ack_policy=AckPolicy.NACK_ON_ERROR)
async def process_order(msg: str, logger: Logger) -> None:
    logger.info(f"Processing: {msg}")
    raise Exception # Message will be nacked and redelivered
```

#### Manual Acknowledgment Handling

```python linenums="1" hl_lines="7 12 14"
from faststream import FastStream, AckPolicy, Logger
from faststream.kafka import KafkaBroker

broker = KafkaBroker()
app = FastStream(broker)

@broker.subscriber("events", ack_policy=AckPolicy.MANUAL)
async def handle_event(msg: str) -> None:
    try:
        # do_smth(msg)
    except Exception:
        await msg.nack()  # or msg.reject()
    else:
        await msg.ack()
```

You can also manage manual acknowledgement using middleware. For more information, [error handling middleware documentation](./middlewares/exception.md){.internal-link}.

### Broker Behavior Summary

However, not all brokers support our semantics. Here is a brief overview of **FastStream's** ACK / NACK / REJECT command mapping to brokers' acknowledgment policies:

| Broker | `ACK` | `NACK` | `REJECT` |
| ------ | ----- | ------ | -------- |
| [RabbitMQ](https://www.rabbitmq.com/docs/confirms#acknowledgement-modes){.external-link target="_blank"} | Protocol ack            | Protocol nack | Protocol reject |
| [NATS JetStream](https://docs.nats.io/using-nats/developer/develop_jetstream#acknowledging-messages){.external-link target="_blank"} | Protocol ack            | Protocol nak  | Protocol term   |
| [Redis Streams](https://redis.io/docs/latest/commands/xack/){.external-link target="_blank"} | Xack call               | Do nothing    | Do nothing      |
| Kafka | Commits offset          | Seek offset and read message again    | Commits offset (same as `ACK`)      |
