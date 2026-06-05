# Request / response

SQS has no native request/reply, so FastStream implements RPC with a dedicated
**response queue** plus a `correlation_id`. Configure the response queue on the
broker, then call `broker.request`:

```python linenums="1"
from faststream.sqs import SQSBroker

broker = SQSBroker(region_name="us-east-1", response_queue="responses")


@broker.subscriber("echo")
async def echo(msg: str) -> str:
    return f"reply: {msg}"


async def call() -> None:
    response = await broker.request("ping", "echo", timeout=10.0)
    assert await response.decode() == "reply: ping"
```

How it works:

1. The producer sends the request with `reply_to` set to the response queue URL
   and a generated `correlation_id`.
2. A background task long-polls the response queue, matching replies to pending
   requests by `correlation_id`, and deletes them once consumed.
3. The handler's return value is published back to `reply_to`.

!!! note ""
    `broker.request` raises `FeatureNotSupportedException` if no `response_queue`
    was configured.
