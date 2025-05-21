from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


class TestArguments(PublisherTestcase):
    broker_factory = RabbitBroker

    def test_just_exchange(self) -> None:
        broker = self.broker_factory("amqp://guest:guest@localhost:5672/vhost")

        @broker.publisher(exchange="test-ex")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="3.0.0").to_jsonable()

        assert schema["channels"] == {
            "_:test-ex:Publisher": {
                "address": "_:test-ex:Publisher",
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.3.0",
                        "exchange": {
                            "autoDelete": False,
                            "durable": False,
                            "name": "test-ex",
                            "type": "direct",
                            "vhost": "/vhost",
                        },
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
                        "$ref": "#/components/messages/_:test-ex:Publisher:Message",
                    },
                },
            },
        }, schema["channels"]

        assert schema["operations"] == {
            "_:test-ex:Publisher": {
                "action": "send",
                "bindings": {
                    "amqp": {
                        "ack": True,
                        "bindingVersion": "0.3.0",
                        "deliveryMode": 1,
                        "mandatory": True,
                    },
                },
                "channel": {
                    "$ref": "#/channels/_:test-ex:Publisher",
                },
                "messages": [
                    {
                        "$ref": "#/channels/_:test-ex:Publisher/messages/Message",
                    },
                ],
            },
        }

    def test_publisher_bindings(self) -> None:
        broker = self.broker_factory()

        @broker.publisher(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.TOPIC),
        )
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="3.0.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "amqp": {
                "bindingVersion": "0.3.0",
                "exchange": {
                    "autoDelete": False,
                    "durable": False,
                    "name": "test-ex",
                    "type": "topic",
                    "vhost": "/",
                },
                "is": "routingKey",
            },
        }

    def test_useless_queue_bindings(self) -> None:
        broker = self.broker_factory()

        @broker.publisher(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.FANOUT),
        )
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="3.0.0").to_jsonable()

        assert schema["channels"] == {
            "_:test-ex:Publisher": {
                "address": "_:test-ex:Publisher",
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.3.0",
                        "exchange": {
                            "autoDelete": False,
                            "durable": False,
                            "name": "test-ex",
                            "type": "fanout",
                            "vhost": "/",
                        },
                        "is": "routingKey",
                    },
                },
                "messages": {
                    "Message": {
                        "$ref": "#/components/messages/_:test-ex:Publisher:Message",
                    },
                },
                "servers": [
                    {
                        "$ref": "#/servers/development",
                    },
                ],
            },
        }

        assert schema["operations"] == {
            "_:test-ex:Publisher": {
                "action": "send",
                "bindings": {
                    "amqp": {
                        "ack": True,
                        "bindingVersion": "0.3.0",
                        "deliveryMode": 1,
                        "mandatory": True,
                    }
                },
                "channel": {"$ref": "#/channels/_:test-ex:Publisher"},
                "messages": [
                    {"$ref": "#/channels/_:test-ex:Publisher/messages/Message"}
                ],
            }
        }

    def test_reusable_exchange(self) -> None:
        broker = self.broker_factory("amqp://guest:guest@localhost:5672/vhost")

        @broker.publisher(exchange="test-ex", routing_key="key1")
        @broker.publisher(exchange="test-ex", routing_key="key2", priority=10)
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="3.0.0").to_jsonable()

        assert schema["channels"] == {
            "key1:test-ex:Publisher": {
                "address": "key1:test-ex:Publisher",
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.3.0",
                        "exchange": {
                            "autoDelete": False,
                            "durable": False,
                            "name": "test-ex",
                            "type": "direct",
                            "vhost": "/vhost",
                        },
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
                        "$ref": "#/components/messages/key1:test-ex:Publisher:Message",
                    },
                },
            },
            "key2:test-ex:Publisher": {
                "address": "key2:test-ex:Publisher",
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.3.0",
                        "exchange": {
                            "autoDelete": False,
                            "durable": False,
                            "name": "test-ex",
                            "type": "direct",
                            "vhost": "/vhost",
                        },
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
                        "$ref": "#/components/messages/key2:test-ex:Publisher:Message",
                    },
                },
            },
        }

        assert schema["operations"] == {
            "key1:test-ex:Publisher": {
                "action": "send",
                "channel": {
                    "$ref": "#/channels/key1:test-ex:Publisher",
                },
                "bindings": {
                    "amqp": {
                        "ack": True,
                        "bindingVersion": "0.3.0",
                        "cc": [
                            "key1",
                        ],
                        "deliveryMode": 1,
                        "mandatory": True,
                    },
                },
                "messages": [
                    {"$ref": "#/channels/key1:test-ex:Publisher/messages/Message"},
                ],
            },
            "key2:test-ex:Publisher": {
                "action": "send",
                "channel": {
                    "$ref": "#/channels/key2:test-ex:Publisher",
                },
                "bindings": {
                    "amqp": {
                        "ack": True,
                        "bindingVersion": "0.3.0",
                        "cc": [
                            "key2",
                        ],
                        "deliveryMode": 1,
                        "priority": 10,
                        "mandatory": True,
                    },
                },
                "messages": [
                    {"$ref": "#/channels/key2:test-ex:Publisher/messages/Message"},
                ],
            },
        }
