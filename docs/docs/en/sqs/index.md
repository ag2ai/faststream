---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# AWS SQS routing

!!! note ""
    **FastStream** SQS support is implemented on top of [**aiobotocore**](https://pypi.org/project/aiobotocore/){.external-link target="_blank"} — an `asyncio` wrapper around `botocore`. You can reach the underlying `aiobotocore` SQS client through `broker.config.client` when you need APIs not wrapped by FastStream.

## Why SQS

[Amazon SQS](https://aws.amazon.com/sqs/){.external-link target="_blank"} is a fully managed message queue. Unlike push-based brokers, SQS is **pull-based**: consumers long-poll a queue for messages, process them, and explicitly delete them when done. SQS offers **standard** (at-least-once, best-effort ordering) and **FIFO** (exactly-once processing, strict ordering) queues.

## FastStream `SQSBroker`

```python linenums="1"
from faststream import FastStream
from faststream.sqs import SQSBroker

broker = SQSBroker(region_name="us-east-1")
app = FastStream(broker)


@broker.subscriber("my-queue")
async def handler(msg: str) -> None:
    print(msg)


@app.after_startup
async def publish_hello() -> None:
    await broker.publish("Hello, SQS!", "my-queue")
```

### Connection parameters

`SQSBroker` accepts the same connection options as `aiobotocore`'s `create_client`:

| Parameter | Description |
|-----------|-------------|
| `region_name` | AWS region, e.g. `"us-east-1"`. |
| `endpoint_url` | Custom endpoint — point this at **LocalStack** (`http://localhost:4566`) or ElasticMQ for local development. |
| `aws_access_key_id` / `aws_secret_access_key` / `aws_session_token` | Explicit credentials (otherwise resolved from the environment / `~/.aws`). |
| `use_ssl`, `verify`, `botocore_config` | Standard botocore client tuning. |
| `response_queue` | Queue used for [RPC replies](rpc.md). |

### Local development with LocalStack

```python linenums="1"
broker = SQSBroker(
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
```

## Testing

Use `TestSQSBroker` to route messages in memory — no AWS connection required:

```python linenums="1"
import pytest
from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker()


@broker.subscriber("test-queue")
async def handler(msg: str) -> str:
    return msg + "!"


@pytest.mark.asyncio
async def test_handler() -> None:
    async with TestSQSBroker(broker) as br:
        await br.publish("hello", "test-queue")
        handler.mock.assert_called_once_with("hello")
```
