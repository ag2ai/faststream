from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase


class TestArguments(ArgumentsTestcase):
    broker_class = RabbitBroker

    def test_subscriber_bindings(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(
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

    def test_subscriber_fanout_bindings(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.FANOUT),
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
                    "type": "fanout",
                    "vhost": "/",
                },
                "is": "routingKey",
            },
        }

    def test_subscriber_headers_bindings(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.HEADERS),
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
                    "type": "headers",
                    "vhost": "/",
                },
                "is": "routingKey",
            },
        }

    def test_subscriber_xdelay_bindings(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.X_DELAYED_MESSAGE),
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
                    "type": "x-delayed-message",
                    "vhost": "/",
                },
                "is": "routingKey",
            },
        }

    def test_subscriber_consistent_hash_bindings(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.X_CONSISTENT_HASH),
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
                    "type": "x-consistent-hash",
                    "vhost": "/",
                },
                "is": "routingKey",
            },
        }

    def test_subscriber_modules_hash_bindings(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.X_MODULUS_HASH),
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
                    "type": "x-modulus-hash",
                    "vhost": "/",
                },
                "is": "routingKey",
            },
        }
