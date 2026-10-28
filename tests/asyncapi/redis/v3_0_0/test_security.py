import ssl

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.redis import RedisBroker
from faststream.security import (
    BaseSecurity,
    SASLPlaintext,
)
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.redis()
def test_base_security_schema(snapshot_json: SnapshotAssertion) -> None:
    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    broker = RedisBroker("rediss://localhost:6379/", security=security)

    assert broker.specification.url == ["rediss://localhost:6379/"]

    schema = get_3_0_0_schema(broker)

    assert schema == snapshot_json


@pytest.mark.redis()
def test_plaintext_security_schema(snapshot_json: SnapshotAssertion) -> None:
    ssl_context = ssl.create_default_context()

    security = SASLPlaintext(
        ssl_context=ssl_context,
        username="admin",
        password="password",
    )

    broker = RedisBroker("redis://localhost:6379/", security=security)

    assert broker.specification.url == ["redis://localhost:6379/"]

    schema = get_3_0_0_schema(broker)

    assert schema == snapshot_json


@pytest.mark.redis()
def test_plaintext_security_schema_without_ssl(
    snapshot_json: SnapshotAssertion,
) -> None:
    security = SASLPlaintext(
        username="admin",
        password="password",
    )

    broker = RedisBroker("redis://localhost:6379/", security=security)

    assert broker.specification.url == ["redis://localhost:6379/"]

    schema = get_3_0_0_schema(broker)

    assert schema == snapshot_json
