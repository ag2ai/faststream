from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.rabbit import (
    RabbitBroker,
    RabbitPublisher,
    RabbitQueue,
    RabbitRoute,
    RabbitRouter,
)
from faststream.specification import Specification
from tests.asyncapi.base.v3_0_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase
from tests.asyncapi.base.v3_0_0.router import RouterTestcase


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

        assert schema == {
            "info": {"title": "FastStream", "version": "0.1.0"},
            "asyncapi": "3.0.0",
            "defaultContentType": "application/json",
            "servers": {
                "development": {
                    "host": "guest:guest@localhost:5672",
                    "pathname": "/",
                    "protocol": "amqp",
                    "protocolVersion": "0.9.1",
                },
            },
            "channels": {
                "test_test:_:Handle": {
                    "address": "test_test:_:Handle",
                    "servers": [{"$ref": "#/servers/development"}],
                    "messages": {
                        "SubscribeMessage": {
                            "$ref": "#/components/messages/test_test:_:Handle:SubscribeMessage",
                        },
                    },
                    "bindings": {
                        "amqp": {
                            "is": "queue",
                            "bindingVersion": "0.3.0",
                            "queue": {
                                "name": "test_test",
                                "durable": False,
                                "exclusive": False,
                                "autoDelete": False,
                                "vhost": "/",
                            },
                        },
                    },
                },
            },
            "operations": {
                "test_test:_:HandleSubscribe": {
                    "action": "receive",
                    "bindings": {
                        "amqp": {
                            "cc": [
                                "test_key",
                            ],
                            "ack": True,
                            "bindingVersion": "0.3.0",
                        },
                    },
                    "messages": [
                        {
                            "$ref": "#/channels/test_test:_:Handle/messages/SubscribeMessage",
                        },
                    ],
                    "channel": {"$ref": "#/channels/test_test:_:Handle"},
                },
            },
            "components": {
                "messages": {
                    "test_test:_:Handle:SubscribeMessage": {
                        "title": "test_test:_:Handle:SubscribeMessage",
                        "correlationId": {
                            "location": "$message.header#/correlation_id",
                        },
                        "payload": {
                            "$ref": "#/components/schemas/Handle:Message:Payload",
                        },
                    },
                },
                "schemas": {
                    "Handle:Message:Payload": {"title": "Handle:Message:Payload"},
                },
            },
        }, schema


@pytest.mark.rabbit()
class TestRouterArguments(ArgumentsTestcase):
    broker_class = RabbitRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(RabbitBroker(routers=[broker]))


@pytest.mark.rabbit()
class TestRouterPublisher(PublisherTestcase):
    broker_class = RabbitRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(RabbitBroker(routers=[broker]))
