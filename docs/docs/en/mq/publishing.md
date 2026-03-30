---
search:
  boost: 10
---

# Publishing

`MQBroker` uses the same `publish` API as the other brokers.

```python linenums="1"
{! docs_src/mq/publishing.py !}
```

## Important IBM MQ specifics

- the destination is always a queue name
- `message_id` and `correlation_id` use MQ-native 48-character hex values when explicitly provided
- custom headers are stored as MQ user properties
