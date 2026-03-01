---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# RabbitMQ Streams

*RabbitMQ* has a [Streams](https://www.rabbitmq.com/streams.html){.external-link target="_blank"} feature, which is closely related to *Kafka* topics.

The main difference from regular *RabbitMQ* queues is that the messages are not deleted after consuming.

And **FastStream** supports this feature as well!

```python linenums="1" hl_lines="4 10-12 17"
{! docs_src/rabbit/subscription/stream.py !}
```
