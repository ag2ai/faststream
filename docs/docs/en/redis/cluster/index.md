---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis Cluster

[Redis Cluster](https://redis.io/docs/management/scaling/){.external-link target="_blank"} provides automatic sharding across multiple Redis nodes. When keys are distributed across cluster slots, a standalone `RedisBroker` connected to a single node will fail with `MOVED` errors for keys that belong to other shards.

**FastStream** provides `RedisClusterBroker` to handle this transparently. It uses `redis.asyncio.cluster.RedisCluster` under the hood, which automatically routes commands to the correct cluster node.

## Subscription Types

`RedisClusterBroker` supports all three subscription types:

- **Streams** — works with `redis-py >= 5.0.0`
- **Lists** — works with `redis-py >= 5.0.0`
- **Channels (Pub/Sub)** — requires `redis-py >= 8.0.0` (async cluster pubsub support)

!!! note
    As of May 2026, `redis-py 8.0` is in beta. To use channel subscribers on a cluster, install the beta: `pip install 'redis>=8.0.0b1'`

## Basic Usage

Replace `RedisBroker` with `RedisClusterBroker`. Point the URL at any node in the cluster:

```python linenums="1"
from faststream import FastStream
from faststream.redis import RedisClusterBroker

broker = RedisClusterBroker("redis://localhost:7000")
app = FastStream(broker)

@broker.subscriber(stream="orders")
async def handle_order(order: dict) -> None:
    print(f"Got order: {order}")

@broker.publisher(stream="confirmations")
@broker.subscriber(list="tasks")
async def process_task(task: str) -> str:
    return f"done: {task}"
```

## Connection Parameters

`RedisClusterBroker` accepts a subset of the parameters from `RedisBroker` — only those supported by `RedisCluster`:

| Parameter | Default | Description |
|---|---|---|
| `url` | `redis://localhost:6379` | Redis Cluster node URL |
| `host` | — | Override host from URL |
| `port` | — | Override port from URL |
| `client_name` | `None` | Client name for `CLIENT SETINFO` |
| `max_connections` | `None` (defaults to `2^31`) | Max connections per node |
| `socket_timeout` | `None` | Socket timeout in seconds |
| `socket_connect_timeout` | `None` | Connection timeout in seconds |
| `socket_keepalive` | `False` | Enable TCP keepalive |
| `encoding` | `utf-8` | String encoding |
| `security` | `None` | Security/TLS options |

Parameters not supported by `RedisCluster` (such as `db`, `socket_read_size`, `retry_on_timeout`, `parser_class`, `encoder_class`) are automatically filtered out.

## FastAPI Integration

Use `RedisClusterRouter` as a drop-in replacement for `RedisRouter`:

```python linenums="1"
from fastapi import FastAPI
from faststream.redis.fastapi import RedisClusterRouter

router = RedisClusterRouter("redis://localhost:7000")

@router.subscriber(stream="events")
async def handle_event(event: dict) -> None:
    print(event)

app = FastAPI()
app.include_router(router)
```

## Testing

Use `TestRedisClusterBroker` to test without a real Redis Cluster:

```python linenums="1"
import pytest
from faststream.redis import RedisClusterBroker, TestRedisClusterBroker

broker = RedisClusterBroker()

@broker.subscriber(stream="test")
async def handler(msg: str) -> None:
    ...

@pytest.mark.asyncio
async def test_publish():
    async with TestRedisClusterBroker(broker) as br:
        await br.publish("hello", stream="test")
```

## Docker Compose

A single-node Redis Cluster for local development:

```yaml
services:
  redis-cluster:
    image: redis:alpine
    command: >-
      sh -c "redis-server
      --port 7000
      --cluster-enabled yes
      --cluster-config-file nodes.conf
      --cluster-node-timeout 5000
      --appendonly yes
      & sleep 2
      && redis-cli --cluster create 127.0.0.1:7000 --cluster-yes
      && wait"
    ports:
      - 7000:7000
```
