from typing import Any

from faststream.security import BaseSecurity, SASLPlaintext


def parse_security(security: BaseSecurity | None) -> dict[str, Any]:
    """Map a FastStream security object onto aiobotocore client kwargs."""
    if security is None:
        return {}
    if isinstance(security, SASLPlaintext):
        return _parse_sasl_plaintext(security)
    if isinstance(security, BaseSecurity):
        return _parse_base_security(security)
    msg = f"SQSBroker does not support {type(security)}"
    raise NotImplementedError(msg)


def _parse_base_security(security: BaseSecurity) -> dict[str, Any]:
    return {"use_ssl": security.use_ssl}


def _parse_sasl_plaintext(security: SASLPlaintext) -> dict[str, Any]:
    return {
        **_parse_base_security(security),
        "aws_access_key_id": security.username,
        "aws_secret_access_key": security.password,
    }
