from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v3_0_0.arguments import ArgumentsTestcase


class TestArguments(ArgumentsTestcase):
    broker_factory = RabbitBroker

    def test_subscriber_bindings(self) -> None:
        broker = self.broker_factory()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.TOPIC),
        )
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="3.0.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "amqp": {
                "bindingVersion": "0.3.0",
                "is": "queue",
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
        broker = self.broker_factory()

        @broker.subscriber(
            RabbitQueue("test", auto_delete=True),
            RabbitExchange("test-ex", type=ExchangeType.FANOUT),
        )
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="3.0.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "amqp": {
                "bindingVersion": "0.3.0",
                "queue": {
                    "autoDelete": True,
                    "durable": False,
                    "exclusive": False,
                    "name": "test",
                    "vhost": "/",
                },
                "is": "queue",
            },
        }
