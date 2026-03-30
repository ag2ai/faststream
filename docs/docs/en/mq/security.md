---
search:
  boost: 10
---

# IBM MQ Security

The current IBM MQ integration supports username/password authentication through FastStream security objects.

```python linenums="1"
{! docs_src/mq/security/plaintext.py !}
```

## Current scope

- `SASLPlaintext` can be used to provide MQ credentials
- TLS and certificate-based IBM MQ setup is not implemented yet and is tracked as a follow-up item
