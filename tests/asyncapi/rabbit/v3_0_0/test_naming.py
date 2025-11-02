import pytest

from faststream.rabbit import RabbitBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.rabbit()
class TestNaming(NamingTestCase):
    broker_class: type[RabbitBroker] = RabbitBroker

    def test_subscriber_with_exchange(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test", "exchange")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["test:exchange:Handle"]

        assert list(schema["components"]["messages"].keys()) == [
            "test:exchange:Handle:SubscribeMessage",
        ]

    def test_publisher_with_exchange(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", "exchange")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["test:exchange:Publisher"]

        assert list(schema["components"]["messages"].keys()) == [
            "test:exchange:Publisher:Message",
        ]

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == {
            "asyncapi": "3.0.0",
            "defaultContentType": "application/json",
            "info": {"title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "host": "guest:guest@localhost:5672",
                    "pathname": "/",
                    "protocol": "amqp",
                    "protocolVersion": "0.9.1",
                },
            },
            "channels": {
                "test:_:Handle": {
                    "address": "test:_:Handle",
                    "servers": [
                        {
                            "$ref": "#/servers/development",
                        },
                    ],
                    "bindings": {
                        "amqp": {
                            "is": "queue",
                            "bindingVersion": "0.3.0",
                            "queue": {
                                "name": "test",
                                "durable": False,
                                "exclusive": False,
                                "autoDelete": False,
                                "vhost": "/",
                            },
                        },
                    },
                    "messages": {
                        "SubscribeMessage": {
                            "$ref": "#/components/messages/test:_:Handle:SubscribeMessage",
                        },
                    },
                },
            },
            "operations": {
                "test:_:Handle": {
                    "action": "receive",
                    "bindings": {
                        "amqp": {
                            "ack": True,
                            "bindingVersion": "0.3.0",
                            "cc": [
                                "test",
                            ],
                        },
                    },
                    "channel": {
                        "$ref": "#/channels/test:_:Handle",
                    },
                    "messages": [
                        {
                            "$ref": "#/channels/test:_:Handle/messages/SubscribeMessage",
                        },
                    ],
                },
            },
            "components": {
                "messages": {
                    "test:_:Handle:SubscribeMessage": {
                        "title": "test:_:Handle:SubscribeMessage",
                        "correlationId": {
                            "location": "$message.header#/correlation_id",
                        },
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                    },
                },
                "schemas": {"EmptyPayload": {"title": "EmptyPayload", "type": "null"}},
            },
        }
