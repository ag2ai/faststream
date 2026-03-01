from typing import Any

from faststream.exceptions import SetupError
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

    if security.ssl_context:
        msg = "ssl_context is not supported by confluent-kafka-python, please use config instead."
        raise SetupError(msg)

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
        "security.protocol": "ssl" if security.use_ssl else "plaintext",
    }


def _parse_sasl_plaintext(security: SASLPlaintext) -> dict[str, Any]:
    return {
        "security.protocol": "sasl_ssl" if security.use_ssl else "sasl_plaintext",
        "sasl.mechanism": "PLAIN",
        "sasl.username": security.username,
        "sasl.password": security.password,
    }


def _parse_sasl_scram256(security: SASLScram256) -> dict[str, Any]:
    return {
        "security.protocol": "sasl_ssl" if security.use_ssl else "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": security.username,
        "sasl.password": security.password,
    }


def _parse_sasl_scram512(security: SASLScram512) -> dict[str, Any]:
    return {
        "security.protocol": "sasl_ssl" if security.use_ssl else "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": security.username,
        "sasl.password": security.password,
    }


def _parse_sasl_oauthbearer(security: SASLOAuthBearer) -> dict[str, Any]:
    return {
        "security.protocol": "sasl_ssl" if security.use_ssl else "sasl_plaintext",
        "sasl.mechanism": "OAUTHBEARER",
    }


def _parse_sasl_gssapi(security: SASLGSSAPI) -> dict[str, Any]:
    return {
        "security.protocol": "sasl_ssl" if security.use_ssl else "sasl_plaintext",
        "sasl.mechanism": "GSSAPI",
    }
