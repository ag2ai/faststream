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

Provide `BaseSecurity` with an SSL context when you need custom TLS settings. A secure URL cannot be downgraded by `BaseSecurity(use_ssl=False)`.

## Username and password

Put percent-encoded credentials in the URL, or use **`SASLPlaintext`** from `faststream.security`. Credentials from `SASLPlaintext` override credentials from the URL.

```python linenums="1" hl_lines="4 5 6"
{! docs_src/mqtt/security/plaintext.py !}
```

Unsupported `security` subclasses raise `NotImplementedError` at broker construction time.

!!! note
    MQTT connection URLs support only `mqtt://` and `mqtts://` TCP/TLS endpoints. Paths, query parameters, and fragments are rejected.
