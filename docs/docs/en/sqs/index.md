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
{! docs_src/sqs/index/basic.py [ln:3-17] !}
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
{! docs_src/sqs/index/localstack.py [ln:6-11] !}
```

## Testing

Use `TestSQSBroker` to route messages in memory — no AWS connection required:

```python linenums="1"
{! docs_src/sqs/index/testing.py !}
```
