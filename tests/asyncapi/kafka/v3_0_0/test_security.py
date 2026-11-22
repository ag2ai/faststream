import ssl

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.kafka import KafkaBroker
from faststream.security import (
    SASLGSSAPI,
    BaseSecurity,
    SASLOAuthBearer,
    SASLPlaintext,
    SASLScram256,
    SASLScram512,
)
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


def _broker_with_handlers(security: BaseSecurity | None = None) -> KafkaBroker:
    broker = KafkaBroker("localhost:9092", security=security)

    @broker.publisher("test_2")
    @broker.subscriber("test_1")
    async def test_topic(msg: str) -> str:
        pass

    return broker


@pytest.mark.kafka()
def test_base_security_schema(snapshot_json: SnapshotAssertion) -> None:
    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    schema = get_3_0_0_schema(_broker_with_handlers(security))

    assert schema == snapshot_json


@pytest.mark.kafka()
def test_plaintext_security_schema(snapshot_json: SnapshotAssertion) -> None:
    ssl_context = ssl.create_default_context()
    security = SASLPlaintext(
        ssl_context=ssl_context,
        username="admin",
        password="password",
    )

    schema = get_3_0_0_schema(_broker_with_handlers(security))

    assert schema == snapshot_json


@pytest.mark.kafka()
def test_scram256_security_schema(snapshot_json: SnapshotAssertion) -> None:
    ssl_context = ssl.create_default_context()
    security = SASLScram256(
        ssl_context=ssl_context,
        username="admin",
        password="password",
    )

    schema = get_3_0_0_schema(_broker_with_handlers(security))

    assert schema == snapshot_json


@pytest.mark.kafka()
def test_scram512_security_schema(snapshot_json: SnapshotAssertion) -> None:
    ssl_context = ssl.create_default_context()
    security = SASLScram512(
        ssl_context=ssl_context,
        username="admin",
        password="password",
    )

    schema = get_3_0_0_schema(_broker_with_handlers(security))

    assert schema == snapshot_json


@pytest.mark.kafka()
def test_oauthbearer_security_schema(snapshot_json: SnapshotAssertion) -> None:
    ssl_context = ssl.create_default_context()
    security = SASLOAuthBearer(
        ssl_context=ssl_context,
    )

    schema = get_3_0_0_schema(_broker_with_handlers(security))

    assert schema == snapshot_json


@pytest.mark.kafka()
def test_gssapi_security_schema(snapshot_json: SnapshotAssertion) -> None:
    ssl_context = ssl.create_default_context()
    security = SASLGSSAPI(
        ssl_context=ssl_context,
    )

    schema = get_3_0_0_schema(_broker_with_handlers(security))

    assert schema == snapshot_json
