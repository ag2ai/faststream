---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Redis Pipeline

**FastStream** supports [**Redis** pipelining](https://redis.io/docs/latest/develop/use/pipelining/){.external-link target="_blank"} to optimize performance when publishing multiple messages in a batch. This allows you to queue several **Redis** operations and execute them together, reducing network round-trips.

## Usage Example

```python linenums="1" hl_lines="2 11 19 22"
{! docs_src/redis/pipeline/pipeline.py !}
```

## API

You can pass the `pipeline` parameter to the `publish` method to delay the execution of **Redis** commands. The commands will only be executed after you explicitly call `#!python await pipe.execute()`.

The `pipeline` object is injected by the `Pipeline` annotation:

```python
from faststream.redis.annotations import Pipeline
```

`Pipeline` is a **Redis** pipeline object (`redis.asyncio.client.Pipeline`), which is wrapped in a FastStream dependency and will be automatically available in any subscriber.

## Batch Publishing with Pipeline

When using `#!python broker.publish_batch()` in combination with the `pipeline` parameter, all messages sent through the pipeline are queued and processed by the subscriber as a single batch after calling `#!python await pipe.execute()`. This allows the subscriber to handle all messages sent through the pipeline in a single execution, improving the efficiency of batch processing.

## Notes

- With `RedisBroker`, pipelining is supported for all **Redis** queue types, including channels, lists, and streams.
- You can combine supported queue types in a single pipeline.

## Redis Cluster

`RedisClusterBroker` accepts a `redis.asyncio.cluster.ClusterPipeline` through the same `pipeline` parameter. Create it from the client returned by `#!python await broker.connect()`. You can queue list and stream publications with `broker.publish()` or a publisher's `publish()`, and list batches with `broker.publish_batch()` or a batch publisher.

Pipelining alone does not make commands atomic. To combine a state update and a publication atomically, use `transaction=True` and keep every key in the same hash slot. The example uses the shared `{orders}` hash tag for the counter and stream:

```python linenums="1"
{!> docs_src/redis/pipeline/cluster_pipeline.py !}
```

The publication stays queued alongside `INCR` until `#!python await pipe.execute()`. Its result list contains the command results in order: the updated counter followed by the stream entry ID. `transaction=True` requires `redis-py >= 6.2.0`; ordinary cluster pipelines do not provide transaction semantics.

!!! warning "Cluster pipeline restrictions"
    redis-py blocks `PUBLISH` in cluster pipelines. Passing `pipeline=pipe` with a channel raises `RedisClusterException`; FastStream does not publish outside the pipeline as a fallback. Publish to channels without a pipeline instead.

!!! note "WATCH and MULTI"
    With a watched transaction, commands issued after `WATCH` but before `MULTI` execute immediately, following redis-py's behavior. Call `pipe.multi()` before queueing publications that must belong to the transaction.

## Benefits

- Reduces network traffic by batching **Redis** commands.
- Improves performance in high-volume scenarios.
- Fully integrates with **FastStream**'s dependency injection system.
- Allows for efficient batch processing when using `#!python broker.publish_batch()` and `pipeline`, as all messages are processed as a single entity by the subscriber after `#!python await pipe.execute()`.
