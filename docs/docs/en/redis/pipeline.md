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

FastStream supports Redis pipelining to optimize performance when publishing multiple messages in a batch. Pipelining allows you to queue multiple Redis operations and execute them in a single network round-trip, significantly reducing latency.

## Usage Example

```python linenums="1" title="app.py"
--8<-- "examples/redis/pipeline.py"
```

## API

You can pass the pipeline parameter to the publish method to defer execution of the Redis command. The commands will be executed only after you explicitly call pipe.execute().

The pipeline object is injected via Pipeline annotation:

```python
from faststream.redis.annotations import Pipeline
```

Pipeline is a Redis pipeline object (redis.asyncio.client.Pipeline) wrapped in a FastStream dependency, and will be available automatically in any subscriber.

## Notes

- Pipelining is supported for all Redis queue types: channel, list, and stream.
- You can mix multiple queue types within the same pipeline.
- The pipeline argument is mutually exclusive with rpc. Attempting to use both at the same time will raise an error:

```python
raise SetupError(
    "You cannot use both rpc and pipeline arguments at the same time: "
    "select only one delivery mechanism."
)
```

## Benefits

- Reduces network overhead by batching Redis commands.
- Improves message throughput in high-load scenarios.
- Fully integrated with FastStream's dependency injection system.
