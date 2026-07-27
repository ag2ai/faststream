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
{! docs_src/sqs/ack/policy.py [ln:3,6] !}
```

## Manual acknowledgement

```python linenums="1"
{! docs_src/sqs/ack/manual.py [ln:5,11-14] !}
```

## Dead-letter queues

Route exhausted messages to a DLQ with a redrive policy on the queue:

```python linenums="1"
{! docs_src/sqs/ack/dlq.py [ln:4,9-15] !}
```
