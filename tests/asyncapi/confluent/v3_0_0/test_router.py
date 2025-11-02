from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.confluent import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter
from faststream.specification import Specification
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v3_0_0.router import RouterTestcase


@pytest.mark.confluent()
class TestRouter(RouterTestcase):
    broker_class = KafkaBroker
    router_class = KafkaRouter
    route_class = KafkaRoute
    publisher_class = KafkaPublisher

    def test_prefix(self) -> None:
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        @router.subscriber("test")
        async def handle(msg) -> None: ...

        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert schema == {
            "info": {"title": "FastStream", "version": "0.1.0"},
            "asyncapi": "3.0.0",
            "defaultContentType": "application/json",
            "servers": {
                "development": {
                    "host": "localhost",
                    "pathname": "",
                    "protocol": "kafka",
                    "protocolVersion": "auto",
                },
            },
            "channels": {
                "test_test:Handle": {
                    "address": "test_test:Handle",
                    "servers": [{"$ref": "#/servers/development"}],
                    "messages": {
                        "SubscribeMessage": {
                            "$ref": "#/components/messages/test_test:Handle:SubscribeMessage",
                        },
                    },
                    "bindings": {
                        "kafka": {"topic": "test_test", "bindingVersion": "0.4.0"},
                    },
                },
            },
            "operations": {
                "test_test:Handle": {
                    "action": "receive",
                    "messages": [
                        {
                            "$ref": "#/channels/test_test:Handle/messages/SubscribeMessage",
                        },
                    ],
                    "channel": {"$ref": "#/channels/test_test:Handle"},
                },
            },
            "components": {
                "messages": {
                    "test_test:Handle:SubscribeMessage": {
                        "title": "test_test:Handle:SubscribeMessage",
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
        }


@pytest.mark.confluent()
class TestRouterArguments(ArgumentsTestcase):
    broker_class = KafkaRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(KafkaBroker(routers=[broker]))


@pytest.mark.confluent()
class TestRouterPublisher(PublisherTestcase):
    broker_class = KafkaRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(KafkaBroker(routers=[broker]))
