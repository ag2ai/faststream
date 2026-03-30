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

- producer publish paths retry once after reconnectable MQ client failures
- request publish retries are limited to failures before the outbound request is successfully published
- if a reconnectable failure happens after the request was published but before the reply is received, FastStream raises the error instead of replaying the RPC request
- subscribers reopen their queue handle after reconnectable MQ client failures
- the current implementation is queue-first and follows IBM MQ client reconnect semantics rather than implementing its own HA protocol

## Notes

- this HA path is separate from TLS support
- TLS / certificate-based IBM MQ configuration is still tracked as a later follow-up
