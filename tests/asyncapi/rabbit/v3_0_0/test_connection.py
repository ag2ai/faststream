from faststream.rabbit import RabbitBroker
from faststream.specification import Tag
from faststream.specification.asyncapi import AsyncAPI


def test_base() -> None:
    schema = AsyncAPI(
        RabbitBroker(
            "amqps://localhost",
            port=5673,
            protocol_version="0.9.0",
            description="Test description",
            tags=(Tag(name="some-tag", description="experimental"),),
        ),
        schema_version="3.0.0",
    ).to_jsonable()

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "description": "Test description",
                "protocol": "amqps",
                "protocolVersion": "0.9.0",
                "tags": [{"description": "experimental", "name": "some-tag"}],
                "host": "guest:guest@localhost:5673",  # pragma: allowlist secret
                "pathname": "/",
            },
        },
    }


def test_kwargs() -> None:
    broker = RabbitBroker(
        "amqp://guest:guest@localhost:5672/?heartbeat=300",  # pragma: allowlist secret
        host="127.0.0.1",
    )

    assert (
        broker.url
        == "amqp://guest:guest@127.0.0.1:5672/?heartbeat=300"  # pragma: allowlist secret
    )


def test_custom() -> None:
    broker = RabbitBroker(
        "amqps://localhost",
        specification_url="amqp://guest:guest@127.0.0.1:5672/vh",  # pragma: allowlist secret
    )

    broker.publisher("test")
    schema = AsyncAPI(broker, schema_version="3.0.0").to_jsonable()

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {
            "test:_:Publisher": {
                "address": "test:_:Publisher",
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.3.0",
                        "exchange": {"type": "default", "vhost": "/vh"},
                        "is": "routingKey",
                    },
                },
                "servers": [
                    {
                        "$ref": "#/servers/development",
                    },
                ],
                "messages": {
                    "Message": {
                        "$ref": "#/components/messages/test:_:Publisher:Message",
                    },
                },
            },
        },
        "operations": {
            "test:_:Publisher": {
                "action": "send",
                "bindings": {
                    "amqp": {
                        "ack": True,
                        "bindingVersion": "0.3.0",
                        "cc": [
                            "test",
                        ],
                        "deliveryMode": 1,
                        "mandatory": True,
                    },
                },
                "channel": {
                    "$ref": "#/channels/test:_:Publisher",
                },
                "messages": [
                    {
                        "$ref": "#/channels/test:_:Publisher/messages/Message",
                    },
                ],
            },
        },
        "components": {
            "messages": {
                "test:_:Publisher:Message": {
                    "correlationId": {"location": "$message.header#/correlation_id"},
                    "payload": {
                        "$ref": "#/components/schemas/test:_:Publisher:Message:Payload",
                    },
                    "title": "test:_:Publisher:Message",
                },
            },
            "schemas": {"test:_:Publisher:Message:Payload": {}},
        },
        "defaultContentType": "application/json",
        "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "amqp",
                "protocolVersion": "0.9.1",
                "host": "guest:guest@127.0.0.1:5672",  # pragma: allowlist secret
                "pathname": "/vh",  # pragma: allowlist secret
            },
        },
    }
