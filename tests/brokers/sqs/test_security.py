from typing import Any

import pytest

from faststream.security import BaseSecurity, SASLPlaintext, SASLScram256
from faststream.sqs.security import parse_security


@pytest.mark.sqs()
def test_parse_no_security() -> None:
    assert parse_security(None) == {}


@pytest.mark.sqs()
@pytest.mark.parametrize(
    ("security", "expected"),
    (
        pytest.param(BaseSecurity(use_ssl=True), {"use_ssl": True}, id="ssl"),
        pytest.param(BaseSecurity(use_ssl=False), {"use_ssl": False}, id="no ssl"),
        pytest.param(BaseSecurity(), {"use_ssl": False}, id="default"),
        pytest.param(
            SASLPlaintext(username="key", password="secret", use_ssl=True),
            {
                "use_ssl": True,
                "aws_access_key_id": "key",
                "aws_secret_access_key": "secret",
            },
            id="credentials",
        ),
    ),
)
def test_parse_security(security: BaseSecurity, expected: dict[str, Any]) -> None:
    assert parse_security(security) == expected


@pytest.mark.sqs()
def test_unsupported_subclass_falls_back_to_base_security() -> None:
    """Unknown ``BaseSecurity`` subclasses degrade to the plain TLS mapping.

    Same convention as the MQTT broker: only ``SASLPlaintext`` carries
    credentials; anything else contributes ``use_ssl`` only.
    """
    parsed = parse_security(SASLScram256(username="u", password="p"))

    assert parsed == {"use_ssl": False}
