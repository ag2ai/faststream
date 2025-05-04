from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


class TestArguments(PublisherTestcase):
    broker_class = RabbitBroker

    def test_just_exchange(self) -> None:
        broker = self.broker_class("amqp://guest:guest@localhost:5672/vhost")

        @broker.publisher(exchange="test-ex")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        assert schema["channels"] == {
            "_:test-ex:Publisher": {
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.2.0",
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
                "subscribe": {
                    "bindings": {
                        "amqp": {
                            "ack": True,
                            "bindingVersion": "0.2.0",
                            "deliveryMode": 1,
                            "mandatory": True,
                        },
                    },
                    "message": {
                        "$ref": "#/components/messages/_:test-ex:Publisher:Message",
                    },
                },
                "servers": ["development"],
            },
        }, schema["channels"]

    def test_publisher_bindings(self) -> None:
        broker = self.broker_class()

        @broker.publisher(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.TOPIC),
        )
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "amqp": {
                "bindingVersion": "0.2.0",
                "exchange": {
                    "autoDelete": False,
                    "durable": False,
                    "name": "test-ex",
                    "type": "topic",
                    "vhost": "/",
                },
                "is": "routingKey",
                "queue": {
                    "autoDelete": True,
                    "durable": False,
                    "exclusive": False,
                    "name": "test",
                    "vhost": "/",
                },
            },
        }

    def test_useless_queue_bindings(self) -> None:
        broker = self.broker_class()

        @broker.publisher(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.FANOUT),
        )
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        assert schema["channels"] == {
            "_:test-ex:Publisher": {
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.2.0",
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
                "subscribe": {
                    "bindings": {
                        "amqp": {
                            "ack": True,
                            "bindingVersion": "0.2.0",
                            "deliveryMode": 1,
                            "mandatory": True,
                        },
                    },
                    "message": {
                        "$ref": "#/components/messages/_:test-ex:Publisher:Message",
                    },
                },
                "servers": ["development"],
            },
        }

    def test_reusable_exchange(self) -> None:
        broker = self.broker_class("amqp://guest:guest@localhost:5672/vhost")

        @broker.publisher(exchange="test-ex", routing_key="key1")
        @broker.publisher(exchange="test-ex", routing_key="key2", priority=10)
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        assert schema["channels"] == {
            "key1:test-ex:Publisher": {
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.2.0",
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
                "subscribe": {
                    "bindings": {
                        "amqp": {
                            "ack": True,
                            "bindingVersion": "0.2.0",
                            "cc": "key1",
                            "deliveryMode": 1,
                            "mandatory": True,
                        },
                    },
                    "message": {
                        "$ref": "#/components/messages/key1:test-ex:Publisher:Message",
                    },
                },
                "servers": ["development"],
            },
            "key2:test-ex:Publisher": {
                "bindings": {
                    "amqp": {
                        "bindingVersion": "0.2.0",
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
                "subscribe": {
                    "bindings": {
                        "amqp": {
                            "ack": True,
                            "bindingVersion": "0.2.0",
                            "cc": "key2",
                            "deliveryMode": 1,
                            "priority": 10,
                            "mandatory": True,
                        },
                    },
                    "message": {
                        "$ref": "#/components/messages/key2:test-ex:Publisher:Message",
                    },
                },
                "servers": ["development"],
            },
        }, schema["channels"]
