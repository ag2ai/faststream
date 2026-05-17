from typing import Any

from faststream.security import BaseSecurity, SASLPlaintext


def parse_security(security: BaseSecurity | None) -> dict[str, Any]:
    if security is None:
        return {}

    if isinstance(security, SASLPlaintext):
        return {
            "username": security.username,
            "password": security.password,
            "use_ssl": security.use_ssl,
            "ssl_context": security.ssl_context,
        }

    if isinstance(security, BaseSecurity):
        return {
            "use_ssl": security.use_ssl,
            "ssl_context": security.ssl_context,
        }

    msg = f"MQBroker does not support `{type(security)}`."
    raise NotImplementedError(msg)
