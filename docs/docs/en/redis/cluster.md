---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis Cluster Broker

## Overview

**FastStream** provides a `RedisClusterBroker` for working with [**Redis Cluster**](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/){.external-link target="_blank"}. It is a drop-in replacement for `RedisBroker` with the same constructor API, designed for multi-node cluster deployments.

## When to Use

| Use RedisBroker | Use RedisClusterBroker |
|---|---|
| Single Redis instance | Multi-node cluster |
| Development / testing | Production with HA |
| Need `pipeline` support | Can tolerate no pipeline |

## Connecting

A single URL is enough — the cluster auto-discovers the remaining nodes:

```python linenums="1"
from faststream.redis import RedisClusterBroker

broker = RedisClusterBroker(url="redis://node1:7000")
```

For multi-address environments you can explicitly specify seed nodes via `startup_nodes`:

```python linenums="1"
broker = RedisClusterBroker(
    url="redis://node1:7000",
    startup_nodes=[
        ("node2", 7001),
        ("node3", 7002),
    ],
)
```

## Feature Support

| Feature | RedisBroker | RedisClusterBroker |
|---|---|---|
| List | ✅ | ✅ |
| Stream + XAUTOCLAIM | ✅ | ✅ |
| Pub/Sub | ✅ | ✅ (via sync cluster) |
| Pipeline | ✅ | ❌ |

## Stream Location

In Redis Cluster every key (including stream names) is assigned to one of 16384 hash slots, each served by a specific node. Consumer groups and `XREADGROUP` operate within that single node. This is transparent to the client — `RedisCluster` handles routing automatically.

!!! tip "Cross-node reads"
    A stream with a consumer group can only be read from the node that owns its hash slot. The cluster client handles this routing; no manual configuration is needed.

## Migration from RedisBroker

`RedisClusterBroker` accepts the **same parameters** as `RedisBroker`, so migration is a one-line change:

```python
# Before
from faststream.redis import RedisBroker
broker = RedisBroker(url="redis://localhost:6379")

# After
from faststream.redis import RedisClusterBroker
broker = RedisClusterBroker(url="redis://localhost:7000")
```

## Limitations

- **Pipeline** is not supported in Redis Cluster.
- **XAUTOCLAIM** with `min_idle_time` requires a consumer group with `group` and `consumer` parameters on `StreamSub`.
- **Pub/Sub** uses a synchronous `RedisCluster` client (via `ThreadPoolExecutor`) because the async client does not expose `publish`/`pubsub` until `redis-py >= 8.0.0`.

## References

- [Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/){.external-link target="_blank"}
- [Redis Streams with Consumer Groups](https://redis.io/docs/latest/develop/data-types/streams/#consumer-groups){.external-link target="_blank"}
