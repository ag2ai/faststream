from typing import Any

from faststream.security import (
    SASLGSSAPI,
    BaseSecurity,
    SASLOAuthBearer,
    SASLPlaintext,
    SASLScram256,
    SASLScram512,
)


def parse_security(security: BaseSecurity | None) -> dict[str, Any]:
    if security is None:
        return {}
    if isinstance(security, SASLPlaintext):
        return _parse_sasl_plaintext(security)
    if isinstance(security, SASLScram256):
        return _parse_sasl_scram256(security)
    if isinstance(security, SASLScram512):
        return _parse_sasl_scram512(security)
    if isinstance(security, SASLOAuthBearer):
        return _parse_sasl_oauthbearer(security)
    if isinstance(security, SASLGSSAPI):
        return _parse_sasl_gssapi(security)
    if isinstance(security, BaseSecurity):
        return _parse_base_security(security)
    msg = f"KafkaBroker does not support `{type(security)}`."
    raise NotImplementedError(msg)


def _parse_base_security(security: BaseSecurity) -> dict[str, Any]:
    return {
        "security_protocol": "SSL" if security.use_ssl else "PLAINTEXT",
        "ssl_context": security.ssl_context,
    }


def _parse_sasl_plaintext(security: SASLPlaintext) -> dict[str, Any]:
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "ssl_context": security.ssl_context,
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": security.username,
        "sasl_plain_password": security.password,
    }


def _parse_sasl_scram256(security: SASLScram256) -> dict[str, Any]:
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "ssl_context": security.ssl_context,
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_plain_username": security.username,
        "sasl_plain_password": security.password,
    }


def _parse_sasl_scram512(security: SASLScram512) -> dict[str, Any]:
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "ssl_context": security.ssl_context,
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_plain_username": security.username,
        "sasl_plain_password": security.password,
    }


def _parse_sasl_oauthbearer(security: SASLOAuthBearer) -> dict[str, Any]:
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "ssl_context": security.ssl_context,
        "sasl_mechanism": "OAUTHBEARER",
    }


def _parse_sasl_gssapi(security: SASLGSSAPI) -> dict[str, Any]:
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "ssl_context": security.ssl_context,
        "sasl_mechanism": "GSSAPI",
    }
