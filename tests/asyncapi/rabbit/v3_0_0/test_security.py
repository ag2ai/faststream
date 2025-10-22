import ssl

import pytest

from faststream.rabbit import RabbitBroker
from faststream.security import (
    BaseSecurity,
    SASLPlaintext,
)
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.rabbit()
def test_base_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    broker = RabbitBroker("amqp://guest:guest@localhost:5672/", security=security)

    assert broker.specification.url == ["amqps://guest:guest@localhost:5672/"]
    assert broker._connection_kwargs.get("ssl_context") is ssl_context

    schema = get_3_0_0_schema(broker)

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}, "securitySchemes": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "amqps",
                "protocolVersion": "0.9.1",
                "security": [],
                "host": "guest:guest@localhost:5672",
                "pathname": "/",
            },
        },
    }


@pytest.mark.rabbit()
def test_plaintext_security_schema() -> None:
    ssl_context = ssl.create_default_context()

    security = SASLPlaintext(
        ssl_context=ssl_context,
        username="admin",
        password="password",
    )

    broker = RabbitBroker("amqp://guest:guest@localhost/", security=security)

    assert broker.specification.url == ["amqps://admin:password@localhost:5671/"]
    assert broker._connection_kwargs.get("ssl_context") is ssl_context

    schema = get_3_0_0_schema(broker)

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {
            "messages": {},
            "schemas": {},
            "securitySchemes": {"user-password": {"type": "userPassword"}},
        },
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "amqps",
                "protocolVersion": "0.9.1",
                "security": [{"$ref": "#/components/securitySchemes/user-password"}],
                "host": "admin:password@localhost:5671",
                "pathname": "/",
            },
        },
    }


@pytest.mark.rabbit()
def test_plaintext_security_schema_without_ssl() -> None:
    security = SASLPlaintext(
        username="admin",
        password="password",
    )

    broker = RabbitBroker("amqp://guest:guest@localhost:5672/", security=security)

    assert broker.specification.url == ["amqp://admin:password@localhost:5672/"]

    schema = get_3_0_0_schema(broker)

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {
            "messages": {},
            "schemas": {},
            "securitySchemes": {"user-password": {"type": "userPassword"}},
        },
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "amqp",
                "protocolVersion": "0.9.1",
                "security": [{"$ref": "#/components/securitySchemes/user-password"}],
                "host": "admin:password@localhost:5672",
                "pathname": "/",
            },
        },
    }
