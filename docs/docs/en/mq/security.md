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

- client certificate PEM file
- client private key PEM file
- one or more CA PEM files

Use `mq_tls_from_pem(...)` for this mode. FastStream prepares a temporary PKCS12 keystore in Python and connects using MQ-native TLS settings.

If `keystore_password` is omitted, FastStream generates a strong random password for the process-local temporary keystore.

## TLS with a prebuilt MQ key repository

```python linenums="1"
{! docs_src/mq/security/tls_key_repository.py !}
```

Use `mq_tls_from_keystore(...)` if your deployment already provides a PKCS12 keystore.

## Notes

- IBM MQ TLS is configured with `tls=...`, not with Python `ssl_context`
- `security.use_ssl=True` alone is not enough for IBM MQ
- `tls=None` keeps the current non-TLS behavior
