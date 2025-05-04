import ssl

from faststream.rabbit import RabbitBroker
from faststream.security import (
    BaseSecurity,
    SASLPlaintext,
)
from faststream.specification.asyncapi import AsyncAPI


def test_base_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    broker = RabbitBroker("amqp://guest:guest@localhost:5672/", security=security)

    assert (
        broker.url == "amqps://guest:guest@localhost:5672/"  # pragma: allowlist secret
    )  # pragma: allowlist secret
    assert broker._connection_kwargs.get("ssl_context") is ssl_context

    schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}, "securitySchemes": {}},
        "defaultContentType": "application/json",
        "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "amqps",
                "protocolVersion": "0.9.1",
                "security": [],
                "url": "amqps://guest:guest@localhost:5672/",  # pragma: allowlist secret
            },
        },
    }


def test_plaintext_security_schema() -> None:
    ssl_context = ssl.create_default_context()

    security = SASLPlaintext(
        ssl_context=ssl_context,
        username="admin",
        password="password",  # pragma: allowlist secret
    )

    broker = RabbitBroker("amqp://guest:guest@localhost/", security=security)

    assert (
        broker.url
        == "amqps://admin:password@localhost:5671/"  # pragma: allowlist secret
    )  # pragma: allowlist secret
    assert broker._connection_kwargs.get("ssl_context") is ssl_context

    schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()
    assert (
        schema
        == {
            "asyncapi": "2.6.0",
            "channels": {},
            "components": {
                "messages": {},
                "schemas": {},
                "securitySchemes": {"user-password": {"type": "userPassword"}},
            },
            "defaultContentType": "application/json",
            "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "protocol": "amqps",
                    "protocolVersion": "0.9.1",
                    "security": [{"user-password": []}],
                    "url": "amqps://admin:password@localhost:5671/",  # pragma: allowlist secret
                },
            },
        }
    )


def test_plaintext_security_schema_without_ssl() -> None:
    security = SASLPlaintext(
        username="admin",
        password="password",  # pragma: allowlist secret
    )

    broker = RabbitBroker("amqp://guest:guest@localhost:5672/", security=security)

    assert (
        broker.url
        == "amqp://admin:password@localhost:5672/"  # pragma: allowlist secret
    )  # pragma: allowlist secret

    schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()
    assert (
        schema
        == {
            "asyncapi": "2.6.0",
            "channels": {},
            "components": {
                "messages": {},
                "schemas": {},
                "securitySchemes": {"user-password": {"type": "userPassword"}},
            },
            "defaultContentType": "application/json",
            "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "protocol": "amqp",
                    "protocolVersion": "0.9.1",
                    "security": [{"user-password": []}],
                    "url": "amqp://admin:password@localhost:5672/",  # pragma: allowlist secret
                },
            },
        }
    )
