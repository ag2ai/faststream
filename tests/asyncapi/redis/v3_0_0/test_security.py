import ssl

from faststream.redis import RedisBroker
from faststream.security import (
    BaseSecurity,
    SASLPlaintext,
)
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema


def test_base_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    broker = RedisBroker("rediss://localhost:6379/", security=security)

    assert (
        broker.specification.url
        == ["rediss://localhost:6379/"]  # pragma: allowlist secret
    )  # pragma: allowlist secret

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
                "protocol": "rediss",
                "protocolVersion": "custom",
                "security": [],
                "host": "localhost:6379",
                "pathname": "/",
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

    broker = RedisBroker("redis://localhost:6379/", security=security)

    assert (
        broker.specification.url
        == ["redis://localhost:6379/"]  # pragma: allowlist secret
    )  # pragma: allowlist secret

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
                "protocol": "redis",
                "protocolVersion": "custom",
                "security": [{"user-password": []}],
                "host": "localhost:6379",
                "pathname": "/",
            },
        },
    }


def test_plaintext_security_schema_without_ssl() -> None:
    security = SASLPlaintext(
        username="admin",
        password="password",  # pragma: allowlist secret
    )

    broker = RedisBroker("redis://localhost:6379/", security=security)

    assert (
        broker.specification.url
        == ["redis://localhost:6379/"]  # pragma: allowlist secret
    )  # pragma: allowlist secret

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
                "protocol": "redis",
                "protocolVersion": "custom",
                "security": [{"user-password": []}],
                "host": "localhost:6379",
                "pathname": "/",
            },
        },
    }
