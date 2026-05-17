---
search:
  boost: 10
---

# Message Information

Use `MQMessage` when you need access to MQ-specific metadata or the wrapped raw message.

```python linenums="1"
{! docs_src/mq/message.py !}
```

## Useful fields

- `message_id`
- `correlation_id`
- `headers`
- `reply_to`
- `raw_message.queue`
