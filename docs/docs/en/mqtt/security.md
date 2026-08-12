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

Use the URL scheme to select the transport. `mqtt://` uses plain TCP and defaults to port `1883`; `mqtts://` enables TLS and defaults to port `8883`.

For custom TLS settings, pass an SSL context with `BaseSecurity`.

## Username and password

Provide percent-encoded credentials in the URL or use **`SASLPlaintext`** from `faststream.security`.

```python linenums="1" hl_lines="4 5 6"
{! docs_src/mqtt/security/plaintext.py !}
```

Unsupported `security` subclasses raise `NotImplementedError` at broker construction time.

!!! note
    MQTT connection URLs support only `mqtt://` and `mqtts://` TCP/TLS endpoints. Paths, query parameters, and fragments are rejected.
