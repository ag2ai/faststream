from ssl import SSLContext, create_default_context
from typing import Any

import pytest

from faststream.mqtt.security import _parse_base_security
from faststream.security import BaseSecurity

SSL_CONTEXT = create_default_context()


@pytest.mark.parametrize(
    ("ssl_context", "use_ssl", "expected_parse_result"),
    (
        (None, None, {"tls": False}),
        (None, False, {"tls": False}),
        (None, True, {"tls": True}),
        (SSL_CONTEXT, None, {"tls": SSL_CONTEXT}),
        (SSL_CONTEXT, False, {"tls": SSL_CONTEXT}),
        (SSL_CONTEXT, True, {"tls": SSL_CONTEXT}),
    ),
)
@pytest.mark.mqtt()
def test_parse_base_security(
    ssl_context: SSLContext, use_ssl: bool | None, expected_parse_result: dict[str, Any]
) -> None:
    security = BaseSecurity(ssl_context=ssl_context, use_ssl=use_ssl)

    parsed = _parse_base_security(security=security)

    assert expected_parse_result == parsed
