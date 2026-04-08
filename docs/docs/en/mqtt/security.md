---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Security configuration

`MQTTBroker` accepts the same **`security`** object pattern as other FastStream brokers where supported.

## TLS

Provide `BaseSecurity` with an SSL context, or pass `tls=` directly to `MQTTBroker` (`False`, `True`, or `ssl.SSLContext`) — forwarded to `zmqtt.MQTTClient`.

## Username and password

Use constructor `username` / `password`, or **`SASLPlaintext`** from `faststream.security` so credentials are merged consistently with TLS settings.

```python
from faststream.security import SASLPlaintext

security = SASLPlaintext(username="device", password="secret")
broker = MQTTBroker("mqtt.example.com", port=8883, security=security)
```

Unsupported `security` subclasses raise `NotImplementedError` at broker construction time.
