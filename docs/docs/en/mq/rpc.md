---
search:
  boost: 10
---

# RPC over IBM MQ

**FastStream** supports blocking request/reply over IBM MQ using a temporary reply queue created from a model queue.

```python linenums="1"
{! docs_src/mq/rpc.py !}
```

## How it works

- the request message is published to the destination queue
- FastStream creates a dynamic reply queue for the request
- the response is matched using MQ-native `MsgId` -> `CorrelId`

Caller-managed `reply_to` routing is not part of the current IBM MQ request API.
