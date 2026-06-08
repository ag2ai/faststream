from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.specification.base import Specification
from faststream.sqs import SQSBroker, SQSPublisher, SQSRoute, SQSRouter
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v3_0_0.router import RouterTestcase


@pytest.mark.sqs()
class TestRouter(RouterTestcase):
    broker_class = SQSBroker
    router_class = SQSRouter
    route_class = SQSRoute
    publisher_class = SQSPublisher

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
                    "host": "sqs",
                    "pathname": "",
                    "protocol": "sqs",
                    "protocolVersion": "custom",
                }
            },
            "channels": {
                "test_test:Handle": {
                    "address": "test_test:Handle",
                    "servers": [{"$ref": "#/servers/development"}],
                    "messages": {
                        "SubscribeMessage": {
                            "$ref": "#/components/messages/test_test:Handle:SubscribeMessage"
                        }
                    },
                    "bindings": {
                        "sqs": {
                            "queue": {"name": "test_test", "fifo": False},
                            "bindingVersion": "custom",
                        }
                    },
                }
            },
            "operations": {
                "test_test:HandleSubscribe": {
                    "action": "receive",
                    "channel": {"$ref": "#/channels/test_test:Handle"},
                    "bindings": {"sqs": {"bindingVersion": "custom"}},
                    "messages": [
                        {"$ref": "#/channels/test_test:Handle/messages/SubscribeMessage"}
                    ],
                }
            },
            "components": {
                "messages": {
                    "test_test:Handle:SubscribeMessage": {
                        "title": "test_test:Handle:SubscribeMessage",
                        "correlationId": {"location": "$message.header#/correlation_id"},
                        "payload": {
                            "$ref": "#/components/schemas/Handle:Message:Payload"
                        },
                    }
                },
                "schemas": {
                    "Handle:Message:Payload": {"title": "Handle:Message:Payload"}
                },
            },
        }


@pytest.mark.sqs()
class TestRouterArguments(ArgumentsTestcase):
    broker_class = SQSRouter

    def get_spec(self, *broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(SQSBroker(routers=broker))


@pytest.mark.sqs()
class TestRouterPublisher(PublisherTestcase):
    broker_class = SQSRouter

    def get_spec(self, *broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(SQSBroker(routers=broker))
