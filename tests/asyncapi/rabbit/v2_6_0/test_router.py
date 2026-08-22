from typing import Any

import pytest
from dirty_equals import IsPartialDict

from faststream._internal.broker import BrokerUsecase
from faststream.rabbit import (
    RabbitBroker,
    RabbitPublisher,
    RabbitQueue,
    RabbitRoute,
    RabbitRouter,
)
from faststream.specification import Specification
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v2_6_0.router import RouterTestcase


@pytest.mark.rabbit()
class TestRouter(RouterTestcase):
    broker_class = RabbitBroker
    router_class = RabbitRouter
    route_class = RabbitRoute
    publisher_class = RabbitPublisher

    def test_prefix(self) -> None:
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        @router.subscriber(RabbitQueue("test", routing_key="key"))
        async def handle(msg) -> None: ...

        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert schema["channels"] == IsPartialDict({
            "test_test:_:Handle": {
                "servers": ["development"],
                "bindings": {
                    "amqp": {
                        "is": "routingKey",
                        "bindingVersion": "0.2.0",
                        "queue": {
                            "name": "test_test",
                            "durable": True,
                            "exclusive": False,
                            "autoDelete": False,
                            "vhost": "/",
                        },
                        "exchange": {"type": "default", "vhost": "/"},
                    },
                },
                "publish": {
                    "bindings": {
                        "amqp": {
                            "cc": "test_key",
                            "ack": True,
                            "bindingVersion": "0.2.0",
                        },
                    },
                    "message": {
                        "$ref": "#/components/messages/test_test:_:Handle:Message",
                    },
                },
            },
        }), schema["channels"]

    def test_publisher_prefix(self) -> None:
        """A Publisher's channel name agrees with the routing key beneath it.

        Under a Router prefix the name, the queue binding and the `cc` routing
        key are all prefixed, so the published contract names the address
        messages really go to.
        """
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        router.publisher("test")

        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert list(schema["channels"].keys()) == ["test_test:_:Publisher"], schema[
            "channels"
        ]

        channel = schema["channels"]["test_test:_:Publisher"]
        assert channel["bindings"]["amqp"]["queue"]["name"] == "test_test"
        assert channel["subscribe"]["bindings"]["amqp"]["cc"] == "test_test"


@pytest.mark.rabbit()
class TestRouterArguments(ArgumentsTestcase):
    broker_class = RabbitRouter

    def get_spec(self, *broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(RabbitBroker(routers=broker))


@pytest.mark.rabbit()
class TestRouterPublisher(PublisherTestcase):
    broker_class = RabbitRouter

    def get_spec(self, *broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(RabbitBroker(routers=broker))
