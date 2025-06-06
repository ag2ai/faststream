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
{! docs_src/redis/pipeline/pipeline.py !}
```

## API

You can pass the `pipeline` parameter to the `publish` method to defer execution of the Redis command. The commands will be executed only after you explicitly call `pipe.execute()`.

The pipeline object is injected via `Pipeline` annotation:

```python
from faststream.redis.annotations import Pipeline
```

`Pipeline` is a Redis pipeline object (`redis.asyncio.client.Pipeline`) wrapped in a FastStream dependency, and will be available automatically in any subscriber.

## Batch Publishing with Pipeline

When using `broker.publish_batch` in combination with the `pipeline` parameter, all messages sent via the pipeline will be queued and then processed as a single batch by the subscriber after calling `pipe.execute()`. This means that the subscriber will handle all the messages sent through the pipeline in one execution, improving efficiency for batch processing.

## Notes

- Pipelining is supported for all Redis queue types: channel, list, and stream.
- You can mix multiple queue types within the same pipeline.

## Benefits

- Reduces network overhead by batching Redis commands.
- Improves message throughput in high-load scenarios.
- Fully integrated with FastStream's dependency injection system.
- Enables efficient batch processing when using `broker.publish_batch` with `pipeline`, as all messages are handled as a single unit by the subscriber after `pipe.execute()`.
