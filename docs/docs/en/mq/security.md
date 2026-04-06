---
search:
  boost: 10
---

# IBM MQ Security

The IBM MQ integration supports plain username/password authentication and optional MQ-native TLS configuration.

```python linenums="1"
{! docs_src/mq/security/plaintext.py !}
```

## Plain Credentials

- `SASLPlaintext` can be used to provide MQ credentials

## TLS with PEM inputs

```python linenums="1"
{! docs_src/mq/security/tls_pem.py !}
```

Use this mode when you have:

- CA chain PEM file
- client certificate + private key together in a single PEM file

FastStream prepares a temporary MQ key repository and connects using MQ-native TLS settings.

## TLS with a prebuilt MQ key repository

```python linenums="1"
{! docs_src/mq/security/tls_key_repository.py !}
```

Use this mode if your deployment already provides an MQ key repository.

## Notes

- IBM MQ TLS is configured with `tls=...`, not with Python `ssl_context`
- `security.use_ssl=True` alone is not enough for IBM MQ
- `tls=None` keeps the current non-TLS behavior
