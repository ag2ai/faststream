# FIFO queues

FIFO queues guarantee ordering and exactly-once processing within a message
group. Declare one with `FifoQueue` — the `.fifo` suffix is added automatically:

```python linenums="1"
from faststream.sqs import FifoQueue, SQSBroker

broker = SQSBroker(region_name="us-east-1")

orders = FifoQueue(
    name="orders",
    content_based_deduplication=True,
    deduplication_scope="messageGroup",
    fifo_throughput_limit="perMessageGroupId",
)


@broker.subscriber(orders)
async def handler(msg: str) -> None:
    ...
```

## Publishing to FIFO queues

FIFO sends require a `MessageGroupId`. When content-based deduplication is off,
also provide a `deduplication_id`:

```python linenums="1"
await broker.publish(
    "ordered-event",
    orders,
    group_id="customer-42",
    deduplication_id="evt-1001",
)
```

Messages sharing a `group_id` are delivered in strict order; different groups
are processed in parallel.
