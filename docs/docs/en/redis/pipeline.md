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

**FastStream** supports [**Redis** pipelining](https://redis.io/docs/latest/develop/use/pipelining/){.external-link target="_blank"} to optimize performance when publishing multiple messages in a batch. This allows you to queue several **Redis** operations and execute them in one network round-trip, reducing latency significantly.

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

- Pipelining is supported for all **Redis** queue types, including channels, lists, and streams.
- You can combine multiple queue types in a single pipeline.

## Benefits

- Reduces network traffic by batching **Redis** commands.
- Improves performance in high-volume scenarios.
- Fully integrates with **FastStream**'s dependency injection system.
- Allows for efficient batch processing when using `#!python broker.publish_batch()` and `pipeline`, as all messages are processed as a single entity by the subscriber after `#!python await pipe.execute()`.
