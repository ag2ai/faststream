from unittest.mock import Mock

import pytest

from faststream.nats import (
    NatsBroker,
    NatsCredentials,
    NatsJWT,
    NatsNKey,
    NatsSecurity,
    NatsToken,
    NatsUserPassword,
)
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.nats()
def test_token_security_schema() -> None:
    schema = get_3_0_0_schema(
        NatsBroker(security=NatsToken("do-not-expose-this-token")),
    )

    assert schema["components"]["securitySchemes"] == {
        "nats-token": {
            "type": "apiKey",
            "in": "user",
            "description": "NATS authentication token sent as CONNECT auth_token.",
            "x-nats-auth": "token",
        },
    }
    assert schema["servers"]["development"]["security"] == [
        {"$ref": "#/components/securitySchemes/nats-token"},
    ]
    assert "do-not-expose-this-token" not in repr(schema)


@pytest.mark.nats()
@pytest.mark.parametrize(
    ("security", "scheme_name", "expected"),
    (
        (
            NatsUserPassword("user", "password"),
            "nats-user-password",
            {"type": "userPassword"},
        ),
        (
            NatsNKey.from_seed("SU_DO_NOT_EXPOSE"),
            "nats-nkey",
            {
                "type": "asymmetricEncryption",
                "description": (
                    "NATS NKey challenge-response authentication using an "
                    "Ed25519 signature."
                ),
                "x-nats-auth": "nkey",
                "x-nats-algorithm": "ed25519",
            },
        ),
        (
            NatsCredentials.from_file("do-not-expose.creds"),
            "nats-jwt",
            {
                "type": "asymmetricEncryption",
                "description": (
                    "NATS user JWT authentication with NKey challenge signing."
                ),
                "x-nats-auth": "jwt-nkey",
                "x-nats-credential-source": "credentials-file",
            },
        ),
        (
            NatsJWT(Mock(return_value=b"jwt"), Mock(return_value=b"signature")),
            "nats-jwt",
            {
                "type": "asymmetricEncryption",
                "description": (
                    "NATS user JWT authentication with NKey challenge signing."
                ),
                "x-nats-auth": "jwt-nkey",
                "x-nats-credential-source": "callbacks",
            },
        ),
    ),
)
def test_authentication_security_schema(
    security: NatsSecurity,
    scheme_name: str,
    expected: dict[str, str],
) -> None:
    schema = get_3_0_0_schema(NatsBroker(security=security))

    assert schema["components"]["securitySchemes"] == {scheme_name: expected}
    assert schema["servers"]["development"]["security"] == [
        {"$ref": f"#/components/securitySchemes/{scheme_name}"},
    ]
    assert "DO_NOT_EXPOSE" not in repr(schema)
    assert "do-not-expose.creds" not in repr(schema)
