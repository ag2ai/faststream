from unittest.mock import Mock

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.nats import (
    NatsBroker,
    NatsCredentials,
    NatsJWT,
    NatsNKey,
    NatsSecurity,
    NatsToken,
    NatsUserPassword,
)
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.nats()
def test_token_security_schema(snapshot_json: SnapshotAssertion) -> None:
    schema = get_2_6_0_schema(
        NatsBroker(security=NatsToken("do-not-expose-this-token")),
    )

    assert schema == snapshot_json
    assert "do-not-expose-this-token" not in repr(schema)


@pytest.mark.nats()
@pytest.mark.parametrize(
    "security",
    (
        pytest.param(NatsUserPassword("user", "password"), id="user-password"),
        pytest.param(NatsNKey.from_seed("SU_DO_NOT_EXPOSE"), id="nkey"),
        pytest.param(
            NatsCredentials.from_file("do-not-expose.creds"),
            id="credentials-file",
        ),
        pytest.param(
            NatsJWT(Mock(return_value=b"jwt"), Mock(return_value=b"signature")),
            id="jwt-callbacks",
        ),
    ),
)
def test_authentication_security_schema(
    security: NatsSecurity,
    snapshot_json: SnapshotAssertion,
) -> None:
    schema = get_2_6_0_schema(NatsBroker(security=security))

    assert schema == snapshot_json
    assert "DO_NOT_EXPOSE" not in repr(schema)
    assert "do-not-expose.creds" not in repr(schema)
