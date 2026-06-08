# Security

Credentials are usually resolved by `botocore` from the environment, an IAM
role, or `~/.aws/credentials`. You can also pass them explicitly or via a
FastStream security object.

## Explicit credentials

```python linenums="1"
{! docs_src/sqs/security/explicit.py [ln:6-11] !}
```

## Security objects

`SASLPlaintext` maps `username`/`password` onto the access key id / secret, and
`BaseSecurity` controls TLS:

```python linenums="1"
{! docs_src/sqs/security/sasl.py [ln:4,7-14] !}
```

The configured security also drives the generated AsyncAPI schema.
