from faststream.rabbit import RabbitBroker
from faststream.specification import Tag
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


def test_base() -> None:
    schema = get_2_6_0_schema(
        RabbitBroker(
            "amqps://localhost",
            port=5673,
            protocol_version="0.9.0",
            description="Test description",
            tags=(Tag(name="some-tag", description="experimental"),),
        ),
    )

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "description": "Test description",
                "protocol": "amqps",
                "protocolVersion": "0.9.0",
                "tags": [{"description": "experimental", "name": "some-tag"}],
                "url": "amqps://guest:guest@localhost:5673/",
            },
        },
    }


def test_kwargs() -> None:
    broker = RabbitBroker(
        "amqp://guest:guest@localhost:5672/?heartbeat=300",
        host="127.0.0.1",
    )

    assert broker.specification.url == [
        "amqp://guest:guest@127.0.0.1:5672/?heartbeat=300",
    ]


def test_custom() -> None:
    broker = RabbitBroker(
        "amqps://localhost",
        specification_url="amqp://guest:guest@127.0.0.1:5672/vh",
    )

    broker.publisher("test")
    schema = get_2_6_0_schema(broker)

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {
            "test:_:Publisher": {
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.2.0",
                        "exchange": {"type": "default", "vhost": "/vh"},
                        "is": "routingKey",
                        "queue": {
                            "autoDelete": False,
                            "durable": False,
                            "exclusive": False,
                            "name": "test",
                            "vhost": "/vh",
                        },
                    },
                },
                "subscribe": {
                    "bindings": {
                        "amqp": {
                            "ack": True,
                            "bindingVersion": "0.2.0",
                            "cc": "test",
                            "deliveryMode": 1,
                            "mandatory": True,
                        },
                    },
                    "message": {
                        "$ref": "#/components/messages/test:_:Publisher:Message",
                    },
                },
                "servers": ["development"],
            },
        },
        "components": {
            "messages": {
                "test:_:Publisher:Message": {
                    "correlationId": {
                        "location": "$message.header#/correlation_id",
                    },
                    "payload": {
                        "$ref": "#/components/schemas/test:_:PublisherPayload",
                    },
                    "title": "test:_:Publisher:Message",
                },
            },
            "schemas": {"test:_:PublisherPayload": {}},
        },
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "amqp",
                "protocolVersion": "0.9.1",
                "url": "amqp://guest:guest@127.0.0.1:5672/vh",
            },
        },
    }, schema
