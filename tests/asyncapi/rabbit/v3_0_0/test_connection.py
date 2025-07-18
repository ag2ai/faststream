import pytest

from faststream.rabbit import RabbitBroker
from faststream.specification import Tag
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.rabbit()
def test_base() -> None:
    schema = get_3_0_0_schema(
        RabbitBroker(
            "amqps://localhost",
            port=5673,
            protocol_version="0.9.0",
            description="Test description",
            tags=(Tag(name="some-tag", description="experimental"),),
        ),
    )

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "description": "Test description",
                "protocol": "amqps",
                "protocolVersion": "0.9.0",
                "tags": [{"description": "experimental", "name": "some-tag"}],
                "host": "guest:guest@localhost:5673",
                "pathname": "/",
            },
        },
    }


@pytest.mark.rabbit()
def test_kwargs() -> None:
    broker = RabbitBroker(
        "amqp://guest:guest@localhost:5672/?heartbeat=300",
        host="127.0.0.1",
    )

    assert broker.specification.url == [
        "amqp://guest:guest@127.0.0.1:5672/?heartbeat=300",
    ]


@pytest.mark.rabbit()
def test_custom() -> None:
    broker = RabbitBroker(
        "amqps://localhost",
        specification_url="amqp://guest:guest@127.0.0.1:5672/vh",
    )

    broker.publisher("test")
    schema = get_3_0_0_schema(broker)

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
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "amqp",
                "protocolVersion": "0.9.1",
                "host": "guest:guest@127.0.0.1:5672",
                "pathname": "/vh",
            },
        },
    }
