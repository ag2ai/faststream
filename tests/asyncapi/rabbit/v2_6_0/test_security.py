import ssl

import pytest

from faststream.rabbit import RabbitBroker, RabbitExternalAuth
from faststream.security import (
    BaseSecurity,
    SASLPlaintext,
)
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.rabbit()
def test_base_security_schema(snapshot_json) -> None:
    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    broker = RabbitBroker("amqp://guest:guest@localhost:5672/", security=security)

    assert broker.specification.url == ["amqps://guest:guest@localhost:5672/"]
    assert broker._connection_kwargs.get("ssl_context") is ssl_context

    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json


@pytest.mark.rabbit()
def test_plaintext_security_schema(snapshot_json) -> None:
    ssl_context = ssl.create_default_context()

    security = SASLPlaintext(
        ssl_context=ssl_context,
        username="admin",
        password="password",
    )

    broker = RabbitBroker("amqp://guest:guest@localhost/", security=security)

    assert broker.specification.url == ["amqps://admin:password@localhost:5671/"]
    assert broker._connection_kwargs.get("ssl_context") is ssl_context

    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json


@pytest.mark.rabbit()
def test_plaintext_security_schema_without_ssl(snapshot_json) -> None:
    security = SASLPlaintext(
        username="admin",
        password="password",
    )

    broker = RabbitBroker("amqp://guest:guest@localhost:5672/", security=security)

    assert broker.specification.url == ["amqp://admin:password@localhost:5672/"]

    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json


@pytest.mark.rabbit()
def test_external_auth_security_schema(snapshot_json) -> None:
    ssl_context = ssl.create_default_context()
    security = RabbitExternalAuth(ssl_context=ssl_context)

    broker = RabbitBroker("amqp://guest:guest@localhost/", security=security)

    assert broker.specification.url == ["amqps://localhost:5671/?auth=EXTERNAL"]
    assert broker._connection_kwargs.get("ssl_context") is ssl_context

    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json
