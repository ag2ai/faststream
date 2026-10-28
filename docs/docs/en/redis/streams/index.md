---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
title: "Redis Streams in Python: Producers and Consumers"
description: >-
  Build Redis Streams producers and consumers in Python with redis-py and FastStream. Run an async example, then add consumer groups, acknowledgements and recovery.
search:
  boost: 10
---

# Redis Streams in Python: Producers and Consumers

Use **Redis Streams in Python** to append messages, read them asynchronously and distribute work across a consumer group. FastStream uses **redis-py** for Redis access and adds subscriber and publisher decorators, message validation and in-memory testing. The example below publishes a message and processes it with a stream subscriber.

[Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/){.external-link target="_blank"} are append-only logs introduced in **Redis 5.0**. Each entry has an ID and a set of field-value pairs. Unlike Pub/Sub, entries remain available for later reads until they are deleted or trimmed; durability across server restarts depends on your Redis persistence configuration.

A stream key is stored on one Redis node. Consumer groups distribute entries across workers, but do not partition one stream into Kafka-style partitions. Completion order can differ from stream order when workers process messages concurrently.

## Run a Python producer and consumer

Install the Redis backend and CLI:

```bash
pip install "faststream[redis,cli]"
```

With Redis listening on `localhost:6379`, save this application as `redis_streams.py`:

```python linenums="1"
{! docs_src/redis/stream/group.py !}
```

Start it with:

```bash
faststream run redis_streams:app
```

After startup, the application publishes `"Hi!"` to `test-stream`. The subscriber reads it as part of `test-group` and logs the message. FastStream acknowledges successfully processed messages by default. To publish another message from Python, use `await broker.publish("Hi!", stream="test-stream")` while the broker is connected.

## Choose how to read and recover messages

- [Stream subscriptions](subscription.md): read entries with `XREAD`, or configure `XREADGROUP` for a group.
- [Consumer groups](groups.md): share work across workers with distinct consumer names.
- [Acknowledgements](ack.md): understand `XACK` and control when processing is confirmed.
- [Pending-message recovery](claiming.md): use `XAUTOCLAIM` to recover work abandoned by a worker.
- [Publishing](publishing.md): append messages with `XADD` and configure stream length.

!!! tip "Redis Cluster"
    In Redis Cluster, stream keys (with consumer groups) reside on a single node determined by the key's hash slot. `RedisClusterBroker` handles routing transparently — all stream operations (`xadd`, `xreadgroup`, `xautoclaim`, `xack`, etc.) are directed to the correct node automatically. See the [Cluster docs](../cluster.md){.internal-link}.
