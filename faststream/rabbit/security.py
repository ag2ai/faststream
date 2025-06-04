from typing import TYPE_CHECKING, Optional

from faststream.security import (
    BaseSecurity,
    SASLPlaintext,
)

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict


def parse_security(security: Optional[BaseSecurity]) -> "AnyDict":
    """Convert security object to connection arguments."""
    if security is None:
        return {}
    if isinstance(security, SASLPlaintext):
        return _parse_sasl_plaintext(security)
    if isinstance(security, BaseSecurity):
        return _parse_base_security(security)
    msg = f"RabbitBroker does not support {type(security)}"
    raise NotImplementedError(msg)


def _parse_base_security(security: BaseSecurity) -> "AnyDict":
    return {
        "ssl": security.use_ssl,
        "ssl_context": security.ssl_context,
    }


def _parse_sasl_plaintext(security: SASLPlaintext) -> "AnyDict":
    return {
        "ssl": security.use_ssl,
        "ssl_context": security.ssl_context,
        "login": security.username,
        "password": security.password,
    }
