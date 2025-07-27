import ssl

import pytest

from faststream.redis import RedisBroker
from faststream.security import (
    BaseSecurity,
    SASLPlaintext,
)
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.redis()
def test_base_security_schema() -> None:
    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    broker = RedisBroker("rediss://localhost:6379/", security=security)

    assert broker.specification.url == ["rediss://localhost:6379/"]

    schema = get_2_6_0_schema(broker)

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}, "securitySchemes": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "rediss",
                "protocolVersion": "custom",
                "security": [],
                "url": "rediss://localhost:6379/",
            },
        },
    }


@pytest.mark.redis()
def test_plaintext_security_schema() -> None:
    ssl_context = ssl.create_default_context()

    security = SASLPlaintext(
        ssl_context=ssl_context,
        username="admin",
        password="password",
    )

    broker = RedisBroker("redis://localhost:6379/", security=security)

    assert broker.specification.url == ["redis://localhost:6379/"]

    schema = get_2_6_0_schema(broker)

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
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
                "url": "redis://localhost:6379/",
            },
        },
    }


@pytest.mark.redis()
def test_plaintext_security_schema_without_ssl() -> None:
    security = SASLPlaintext(
        username="admin",
        password="password",
    )

    broker = RedisBroker("redis://localhost:6379/", security=security)

    assert broker.specification.url == ["redis://localhost:6379/"]

    schema = get_2_6_0_schema(broker)

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
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
                "url": "redis://localhost:6379/",
            },
        },
    }
