# Security

Credentials are usually resolved by `botocore` from the environment, an IAM
role, or `~/.aws/credentials`. You can also pass them explicitly or via a
FastStream security object.

## Explicit credentials

```python linenums="1"
broker = SQSBroker(
    region_name="us-east-1",
    aws_access_key_id="AKIA...",
    aws_secret_access_key="...",
    aws_session_token="...",  # optional, for temporary credentials
)
```

## Security objects

`SASLPlaintext` maps `username`/`password` onto the access key id / secret, and
`BaseSecurity` controls TLS:

```python linenums="1"
from faststream.security import SASLPlaintext

broker = SQSBroker(
    region_name="us-east-1",
    security=SASLPlaintext(
        username="AKIA...",
        password="...",
        use_ssl=True,
    ),
)
```

The configured security also drives the generated AsyncAPI schema.
