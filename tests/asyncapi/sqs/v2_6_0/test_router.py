from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.specification.base import Specification
from faststream.sqs import SQSBroker, SQSPublisher, SQSRoute, SQSRouter
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v2_6_0.router import RouterTestcase


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
            "asyncapi": "2.6.0",
            "defaultContentType": "application/json",
            "servers": {
                "development": {
                    "url": "sqs",
                    "protocol": "sqs",
                    "protocolVersion": "custom",
                }
            },
            "channels": {
                "test_test:Handle": {
                    "servers": ["development"],
                    "bindings": {
                        "sqs": {
                            "queue": {"name": "test_test", "fifo": False},
                            "bindingVersion": "custom",
                        }
                    },
                    "publish": {
                        "bindings": {"sqs": {"bindingVersion": "custom"}},
                        "message": {
                            "$ref": "#/components/messages/test_test:Handle:Message"
                        },
                    },
                }
            },
            "components": {
                "messages": {
                    "test_test:Handle:Message": {
                        "title": "test_test:Handle:Message",
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
