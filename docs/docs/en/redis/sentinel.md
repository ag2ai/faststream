---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis Sentinel Broker

## Overview

**FastStream** provides a `RedisSentinelBroker` for working with [**Redis Sentinel**](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/){.external-link target="_blank"} — Redis' built-in high-availability solution. Sentinel monitors a master/replica set and automatically promotes a replica when the master fails. `RedisSentinelBroker` discovers the current master from the sentinel nodes, so publishers and consumers keep working across a failover without any manual reconfiguration.

Unlike a Redis Cluster, Sentinel does **not** shard data — it is a single logical Redis with automatic master failover. Commands, Pub/Sub, Lists, Streams and Pipelines therefore behave **exactly** like a plain `RedisBroker`; only the way the connection is acquired differs.

## When to Use

| Use RedisBroker | Use RedisSentinelBroker |
|---|---|
| Single Redis instance | Master/replica set behind Sentinel |
| Development / testing | Production with automatic failover |
| Fixed master address | Master address discovered dynamically |

## Connecting

Pass the sentinel `(host, port)` nodes and the monitored master group name:

```python linenums="1"
from faststream.redis import RedisSentinelBroker

broker = RedisSentinelBroker(
    sentinels=[
        ("sentinel-1", 26379),
        ("sentinel-2", 26379),
        ("sentinel-3", 26379),
    ],
    sentinel_master_name="mymaster",
)
```

Both `sentinels` and `sentinel_master_name` are required. Connection options such as `db`, `socket_timeout` or `security` are forwarded to the discovered master connection. Use `sentinel_kwargs` to pass options that apply to the **sentinel** connections themselves:

```python linenums="1"
broker = RedisSentinelBroker(
    sentinels=[("sentinel-1", 26379), ("sentinel-2", 26379)],
    sentinel_master_name="mymaster",
    sentinel_kwargs={"socket_timeout": 1.0},
    db=1,
)
```

## How Failover Works

Under the hood the broker builds the client through `Sentinel(...).master_for(master_name)`, which returns a client backed by a `SentinelConnectionPool`. That pool re-discovers the current master from the sentinels on every reconnect. Because every publisher and stream consumer goes through `connection.client`, they all fail over transparently — a dropped connection during a master promotion is simply re-established against the new master.

## FastAPI Integration

For FastAPI applications use `RedisSentinelRouter`, which builds a `RedisSentinelBroker` under the hood and otherwise behaves like `RedisRouter`:

```python linenums="1"
from faststream.redis.fastapi import RedisSentinelRouter

router = RedisSentinelRouter(
    sentinels=[("sentinel-1", 26379), ("sentinel-2", 26379)],
    sentinel_master_name="mymaster",
)

@router.subscriber("channel-name")
async def handle(msg: str) -> None: ...
```

## Feature Support

| Feature | RedisBroker | RedisSentinelBroker |
|---|---|---|
| List | ✅ | ✅ |
| Stream + XAUTOCLAIM | ✅ | ✅ |
| Pub/Sub | ✅ | ✅ |
| Pipeline | ✅ | ✅ |

## Migration from RedisBroker

`RedisSentinelBroker` accepts the same parameters as `RedisBroker` plus the sentinel-specific ones, so migration only requires pointing the broker at the sentinels instead of a fixed host:

```python
# Before
from faststream.redis import RedisBroker
broker = RedisBroker(url="redis://master-host:6379")

# After
from faststream.redis import RedisSentinelBroker
broker = RedisSentinelBroker(
    sentinels=[("sentinel-1", 26379), ("sentinel-2", 26379)],
    sentinel_master_name="mymaster",
)
```

## References

- [Redis Sentinel Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/){.external-link target="_blank"}
- [High Availability with Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/#high-availability){.external-link target="_blank"}
