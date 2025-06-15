# Acknowledgment

Since unexpected errors may occur during message processing, **FastStream** provides an `ack_policy` parameter to control how messages are acknowledged. This parameter defines when and how messages should be acknowledged, negatively acknowledged (nacked), or rejected based on the outcome of the message handler.

## AckPolicy

`AckPolicy` is an enumeration (`Enum`) that defines the message acknowledgment strategy in FastStream. It determines how the system should respond after receiving and processing a message.

### Availability

`AckPolicy` is supported by the following brokers:

- Kafka
- NATS
- Redis
- RabbitMQ

### Usage

You must specify the `ack_policy` parameter when initializing a subscriber:

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

Each `AckPolicy` variant below includes behavior examples for both **successful processing** and **error scenarios**. Note that broker-specific behaviors are noted.

| Policy                      | Description                                                                             | On Success                                               | On Error                                              | Broker Notes                                                                 |
| --------------------------- | --------------------------------------------------------------------------------------- | -------------------------------------------------------- | ----------------------------------------------------- | ---------------------------------------------------------------------------- |
| `AckPolicy.ACK_FIRST`       | Acknowledge immediately upon receipt, before processing begins.                         | Message is acked early; may be lost if processing fails. | Still acked despite error; message not redelivered.   | Kafka commits offset; NATS/Redis/Rabbit confirm immediately.                 |
| `AckPolicy.ACK`             | Acknowledge only after processing completes, regardless of success.                     | Ack sent after successful handling.                      | Ack sent anyway; message not redelivered.             | Kafka: offset commit; others: explicit ack.                                  |
| `AckPolicy.REJECT_ON_ERROR` | Reject message if unhandled exception occurs, discarding it permanently; otherwise ack. | Ack after success.                                       | Rejected/discarded; no retry.                         | RabbitMQ/NATS drop message. Kafka: commit offset.                            |
| `AckPolicy.NACK_ON_ERROR`   | Nack (negative ack) on error to allow redelivery; otherwise ack after success.          | Ack after success.                                       | Nack/redeliver.                                       | Redis streams and RabbitMQ will redeliver; Kafka commits offset as fallback. |
| `AckPolicy.DO_NOTHING`      | No automatic ack/nack/reject. User must manually handle completion via message methods. | Nothing until user calls `msg.ack()`.                    | Nothing until user calls `msg.nack()`/`msg.reject()`. | Full manual control across all brokers.                                      |

---

### When to Use

- Use `ACK_FIRST` for high-throughput scenarios where some message loss is acceptable.
- Use `ACK` when you want the message to be acknowledged regardless of success or failure.
- Use `REJECT_ON_ERROR` to discard messages permanently on failure.
- Use `NACK_ON_ERROR` to retry messages on failure.
- Use `DO_NOTHING` for full manual control of message acknowledgment (e.g. calling `message.ack()` manually).

---

### Extended Examples

#### Automatic Retry on Failure

```python linenums="1" hl_lines="7" title="main.py"
from faststream import FastStream, AckPolicy, Logger
from faststream.rabbitmq import RabbitMQBroker

broker = RabbitMQBroker()
app = FastStream(broker)

@broker.subscriber("orders", ack_policy=AckPolicy.NACK_ON_ERROR)
async def process_order(msg: str, logger: Logger):
    try:
        logger.info(f"Processing: {msg}")
        # Processing logic
    except Exception:
        logger.error("Failed to process", exc_info=True)
        # Message will be automatically nacked and retried

@app.after_startup
async def send_order():
    await broker.publish("order:123", "orders")
```

### Manual Acknowledgment Handling

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

### Broker Behavior Summary

| Broker   | `ACK_FIRST` / `ACK`     | `REJECT_ON_ERROR`                      | `NACK_ON_ERROR`                        | `DO_NOTHING`           |
| -------- | ----------------------- | -------------------------------------- | -------------------------------------- | ---------------------- |
| Kafka    | Commits offset          | Treated as successful — commits offset | Treated as successful — commits offset | Manual commit required |
| RabbitMQ | Protocol ack            | Reject drops message                   | Nack requeues                          | Manual ack/nack/reject |
| NATS     | Protocol ack            | Reject drops message                   | Nack requeues or dead-letters          | Manual ack/nack/reject |
| Redis    | XAck/XPending semantics | Drops or moves to dead-letter          | Requeues                               | Manual ack/nack/reject |
