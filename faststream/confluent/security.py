import ssl
from typing import TYPE_CHECKING, Optional

from faststream.exceptions import SetupError
from faststream.security import (
    SASLGSSAPI,
    BaseSecurity,
    SASLOAuthBearer,
    SASLPlaintext,
    SASLScram256,
    SASLScram512,
)

if TYPE_CHECKING:
    from faststream.types import AnyDict


def parse_security(security: Optional[BaseSecurity]) -> "AnyDict":
    if security and isinstance(security.ssl_context, ssl.SSLContext):
        raise SetupError(
            "ssl_context in not supported by confluent-kafka-python, please use config instead."
        )

    if security is None:
        return {}
    elif isinstance(security, SASLPlaintext):
        return _parse_sasl_plaintext(security)
    elif isinstance(security, SASLScram256):
        return _parse_sasl_scram256(security)
    elif isinstance(security, SASLScram512):
        return _parse_sasl_scram512(security)
    elif isinstance(security, SASLOAuthBearer):
        return _parse_sasl_oauthbearer(security)
    elif isinstance(security, SASLGSSAPI):
        return _parse_sasl_gssapi(security)
    elif isinstance(security, BaseSecurity):
        return _parse_base_security(security)
    else:
        raise NotImplementedError(f"KafkaBroker does not support `{type(security)}`.")


def _parse_base_security(security: BaseSecurity) -> "AnyDict":
    return {
        "security_protocol": "SSL" if security.use_ssl else "PLAINTEXT",
    }


def _parse_sasl_plaintext(security: SASLPlaintext) -> "AnyDict":
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "security_config": {
            "sasl.mechanism": "PLAIN",
            "sasl.username": security.username,
            "sasl.password": security.password,
        },
    }


def _parse_sasl_scram256(security: SASLScram256) -> "AnyDict":
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "security_config": {
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": security.username,
            "sasl.password": security.password,
        },
    }


def _parse_sasl_scram512(security: SASLScram512) -> "AnyDict":
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "security_config": {
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": security.username,
            "sasl.password": security.password,
        },
    }


def _parse_sasl_oauthbearer(security: SASLOAuthBearer) -> "AnyDict":
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "security_config": {
            "sasl.mechanism": "OAUTHBEARER",
        },
    }


def _parse_sasl_gssapi(security: SASLGSSAPI) -> "AnyDict":
    return {
        "security_protocol": "SASL_SSL" if security.use_ssl else "SASL_PLAINTEXT",
        "security_config": {
            "sasl.mechanism": "GSSAPI",
        },
    }
