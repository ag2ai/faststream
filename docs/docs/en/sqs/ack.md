# Acknowledgement

SQS acknowledgement maps onto the queue API:

| FastStream | SQS action | Effect |
|------------|-----------|--------|
| `ack` | `DeleteMessage` | Message handled — removed from the queue. |
| `nack` | `ChangeMessageVisibility(0)` | Returned immediately for redelivery. |
| `reject` | `DeleteMessage` | Dropped (route to a DLQ via a redrive policy). |

The default [`AckPolicy`](../getting-started/acknowledgement.md) for SQS
subscribers is `ACK` (the message is deleted after the handler succeeds, giving
at-least-once delivery). On error the message is **not** deleted and SQS
redelivers it once its visibility timeout expires.

```python linenums="1"
from faststream import AckPolicy

broker = SQSBroker(region_name="us-east-1", ack_policy=AckPolicy.NACK_ON_ERROR)
```

## Manual acknowledgement

```python linenums="1"
from faststream.sqs.annotations import SQSMessage


@broker.subscriber("my-queue", ack_policy=AckPolicy.MANUAL)
async def handler(body: str, msg: SQSMessage) -> None:
    await msg.ack()    # DeleteMessage
    # or: await msg.nack()  / await msg.reject()
```

## Dead-letter queues

Route exhausted messages to a DLQ with a redrive policy on the queue:

```python linenums="1"
from faststream.sqs import RedrivePolicy, SQSQueue

queue = SQSQueue(
    name="orders",
    redrive_policy=RedrivePolicy(
        dead_letter_target_arn="arn:aws:sqs:us-east-1:000000000000:orders-dlq",
        max_receive_count=5,
    ),
)
```
