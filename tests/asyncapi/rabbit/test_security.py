import ssl

from faststream.app import FastStream
from faststream.asyncapi.generate import get_app_schema
from faststream.rabbit import RabbitBroker
from faststream.security import (
    BaseSecurity,
    SASLPlaintext,
)


def test_base_security_schema():
    ssl_context = ssl.create_default_context()
    security = BaseSecurity(ssl_context=ssl_context)

    broker = RabbitBroker("amqp://guest:guest@localhost:5672/", security=security)

    assert broker.url == "amqps://guest:guest@localhost:5672/"
    assert broker._connection_kwargs.get("ssl_context") is ssl_context

    schema = get_app_schema(FastStream(broker)).to_jsonable()

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
                "url": "amqps://guest:guest@localhost:5672/",
            }
        },
    }


def test_plaintext_security_schema():
    ssl_context = ssl.create_default_context()

    security = SASLPlaintext(
        ssl_context=ssl_context,
        username="admin",
        password="password",  # pragma: allowlist secret
    )

    broker = RabbitBroker("amqp://guest:guest@localhost:5672/", security=security)

    assert broker.url == "amqps://admin:password@localhost:5672/"
    assert broker._connection_kwargs.get("ssl_context") is ssl_context

    schema = get_app_schema(FastStream(broker)).to_jsonable()
    assert schema == {
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
                "url": "amqps://admin:password@localhost:5672/",
            }
        },
    }
