from typing import TYPE_CHECKING, Any, Optional

from faststream.security import BaseSecurity, SASLPlaintext

if TYPE_CHECKING:
    from ssl import SSLContext


class RabbitExternalAuth(BaseSecurity):
    """RabbitMQ SASL EXTERNAL authentication using a client TLS certificate."""

    __slots__ = ("ssl_context", "use_ssl")

    def __init__(
        self,
        ssl_context: Optional["SSLContext"] = None,
        use_ssl: bool = True,
    ) -> None:
        super().__init__(
            ssl_context=ssl_context,
            use_ssl=use_ssl,
        )

    def get_requirement(self) -> list[dict[str, Any]]:
        """Get the security requirements for X.509 authentication."""
        return [{"rabbitmq-external": []}]

    def get_schema(self) -> dict[str, dict[str, str]]:
        """Get the security schema for X.509 authentication."""
        return {"rabbitmq-external": {"type": "X509"}}


def parse_security(security: BaseSecurity | None) -> dict[str, Any]:
    """Convert security object to connection arguments."""
    if security is None:
        return {}
    if isinstance(security, RabbitExternalAuth):
        return _parse_external_auth(security)
    if isinstance(security, SASLPlaintext):
        return _parse_sasl_plaintext(security)
    if isinstance(security, BaseSecurity):
        return _parse_base_security(security)
    msg = f"RabbitBroker does not support {type(security)}"
    raise NotImplementedError(msg)


def _parse_base_security(security: BaseSecurity) -> dict[str, Any]:
    return {
        "ssl": security.use_ssl,
        "ssl_context": security.ssl_context,
    }


def _parse_sasl_plaintext(security: SASLPlaintext) -> dict[str, Any]:
    return {
        "ssl": security.use_ssl,
        "ssl_context": security.ssl_context,
        "login": security.username,
        "password": security.password,
    }


def _parse_external_auth(security: RabbitExternalAuth) -> dict[str, Any]:
    return {
        "ssl": security.use_ssl,
        "ssl_context": security.ssl_context,
        "auth": "EXTERNAL",
    }
