import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.confluent import KafkaBroker
from faststream.security import (
    SASLGSSAPI,
    BaseSecurity,
    SASLOAuthBearer,
    SASLPlaintext,
    SASLScram256,
    SASLScram512,
)
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.confluent()
@pytest.mark.parametrize(
    "security",
    (
        pytest.param(BaseSecurity(use_ssl=True), id="BaseSecurity"),
        pytest.param(
            SASLPlaintext(
                username="admin",
                password="password",
                use_ssl=True,
            ),
            id="SASLPlaintext",
        ),
        pytest.param(
            SASLScram256(
                username="admin",
                password="password",
                use_ssl=True,
            ),
            id="SASLScram256",
        ),
        pytest.param(
            SASLScram512(
                username="admin",
                password="password",
                use_ssl=True,
            ),
            id="SASLScram512",
        ),
        pytest.param(SASLOAuthBearer(use_ssl=True), id="SASLOAuthBearer"),
        pytest.param(SASLGSSAPI(use_ssl=True), id="SASLGSSAPI"),
    ),
)
def test_security_schema(
    security: BaseSecurity,
    snapshot_json: SnapshotAssertion,
) -> None:
    broker = KafkaBroker(security=security)
    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json
