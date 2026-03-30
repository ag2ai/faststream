---
search:
  boost: 10
---

# High Availability

IBM MQ high-availability support in **FastStream** uses IBM MQ client-native reconnect options together with a CCDT file.

```python linenums="1"
{! docs_src/mq/ha.py !}
```

## Recommended approach

- define client connection entries in a CCDT file
- point `MQBroker` at that file with `ccdt_url`
- enable reconnect behavior with `reconnect="qmgr"`

## Current behavior

- producer publish and request paths retry once after reconnectable MQ client failures
- subscribers reopen their queue handle after reconnectable MQ client failures
- the current implementation is queue-first and follows IBM MQ client reconnect semantics rather than implementing its own HA protocol

## Notes

- this HA path is separate from TLS support
- TLS / certificate-based IBM MQ configuration is still tracked as a later follow-up
