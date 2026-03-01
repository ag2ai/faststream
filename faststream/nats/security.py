from typing import Any

from faststream.security import BaseSecurity, SASLPlaintext


def parse_security(security: BaseSecurity | None) -> dict[str, Any]:
    if security is None:
        return {}
    if isinstance(security, SASLPlaintext):
        return _parse_sasl_plaintext(security)
    if isinstance(security, BaseSecurity):
        return _parse_base_security(security)
    msg = f"NatsBroker does not support {type(security)}"
    raise NotImplementedError(msg)


def _parse_base_security(security: BaseSecurity) -> dict[str, Any]:
    return {
        "tls": security.ssl_context,
    }


def _parse_sasl_plaintext(security: SASLPlaintext) -> dict[str, Any]:
    return {
        "tls": security.ssl_context,
        "user": security.username,
        "password": security.password,
    }
