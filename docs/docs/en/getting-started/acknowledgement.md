# Acknowledgment

Due to the possibility of unexpected errors during message processing, FastStream provides an `ack_policy` parameter that allows users to control how messages are handled. This parameter determines when and how messages should be acknowledged or rejected based on the result of the message processing.

## AckPolicy

`AckPolicy` is an enumerated type (`Enum`) in FastStream that specifies the message acknowledgment strategy. It determines how the system responds after receiving and processing a message.

### Availability

`AckPolicy` is supported by the following brokers:

- Kafka
- NATS
- Redis
- RabbitMQ

### Usage

You must specify the `ack_policy` parameter when creating a subscriber:

```python linenums="1" hl_lines="9" title="main.py"
from faststream import FastStream, Logger, AckPolicy
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream(broker)

@broker.subscriber(
    "test",
    ack_policy=AckPolicy.REJECT_ON_ERROR,
)
async def handler(msg: str, logger: Logger):
    logger.info(msg)
```

### Available Options

Each `AckPolicy` variant includes behavior examples for both successful processing and error scenarios. Note that broker-specific behaviors are also included.

| Policy                      | Description                                                                                                                             | On Success                                                                   | On Error                                              | Broker Notes                                                             |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------ |
| `AckPolicy.ACK_FIRST`       | Acknowledge immediately upon receipt, before processing begins.                                                                         | Message is acknowledged early; may be lost if processing fails.              | Acknowledged despite error; message not re-delivered. | Kafka commits offset; NATS, Redis, and RabbitMQ acknowledge immediately. |
| `AckPolicy.ACK`             | Acknowledge only after processing completes, regardless of success.                                                                     | Ack sent after successful handling.                                          | Ack sent anyway; message not redelivered.             | Kafka: offset commit; others: explicit ack.                              |
| `AckPolicy.REJECT_ON_ERROR` | Reject message if an unhandled exception occurs, permanently discarding it; otherwise, ack.                                             | Reject after success.                                                        | Message discarded; no retry.                          | RabbitMQ/NATS drops message. Kafka commits offset.                       |
| `AckPolicy.NACK_ON_ERROR`   | Nack on error to allow message redelivery, ack after success otherwise.                                                                 | Ack after success.                                                           | Redeliver; attempt to resend message.                 | Redis streams and RabbitMQ redelivers; Kafka commits as fallback.        |
| `AckPolicy.NO_ACTION`       | No automatic acknowledgement, negative acknowledgement, or rejection. The user must manually handle the completion via message methods. | No action until the user calls `msg.ack()`, `msg.nack()`, or `msg.reject()`. | No action until user calls any of these methods.      | Complete manual control over all brokers.                                |

---

### When to Use

- Use `ACK_FIRST` for scenarios with high throughput where some message loss can be acceptable.
- Use `ACK` if you want the message to be acknowledged, regardless of success or failure.
- Use `REJECT_ON_ERROR` to permanently discard messages on failure.
- Use `NACK_ON_ERROR` to retry messages in case of failure.
- Use `DO_NOTHING` to fully manually control message acknowledgment (for example, calling `#!python message.ack()` yourself).

---

### Extended Examples

#### Automatic Retry on Failure

```python linenums="1" hl_lines="7" title="main.py"
from faststream import FastStream, AckPolicy, Logger
from faststream.rabbitmq import RabbitMQBroker

broker = RabbitMQBroker()
app = FastStream(broker)

class SomeError(Exception):
    pass

@broker.subscriber("orders", ack_policy=AckPolicy.NACK_ON_ERROR)
async def process_order(msg: str, logger: Logger):
    logger.info(f"Processing: {msg}")
    # Processing logic ...
    raise SomeError
    # Message will be automatically nacked and retried

@app.after_startup
async def send_order():
    await broker.publish("order:123", "orders")
```

#### Manual Acknowledgment Handling

```python linenums="1" hl_lines="7 12 14" title="main.py"
from faststream import FastStream, AckPolicy, Logger
from faststream.kafka import KafkaBroker

broker = KafkaBroker()
app = FastStream(broker)

@broker.subscriber("events", ack_policy=AckPolicy.DO_NOTHING)
async def handle_event(msg, logger):
    try:
        logger.info("Received event")
        # Manual acknowledgment required
        await msg.ack()
    except Exception:
        await msg.nack()  # or msg.reject()
```

You can also manage acknowledgment using middleware. For more information, please see our [error handling middleware documentation](./middlewares/exception.md).

### Broker Behavior Summary

| Broker   | `ACK_FIRST` / `ACK`     | `REJECT_ON_ERROR`                      | `NACK_ON_ERROR`                        | `DO_NOTHING`           |
| -------- | ----------------------- | -------------------------------------- | -------------------------------------- | ---------------------- |
| Kafka    | Commits offset          | Treated as successful — commits offset | Treated as successful — commits offset | Manual commit required |
| RabbitMQ | Protocol ack            | Reject drops message                   | Nack requeues                          | Manual ack/nack/reject |
| NATS     | Protocol ack            | Reject drops message                   | Nack requeues or dead-letters          | Manual ack/nack/reject |
| Redis    | XAck/XPending semantics | Drops or moves to dead-letter          | Requeues                               | Manual ack/nack/reject |
