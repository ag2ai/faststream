from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.mqtt import MQTTBroker, MQTTPublisher, MQTTRoute, MQTTRouter
from faststream.specification.base import Specification
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v2_6_0.router import RouterTestcase


@pytest.mark.mqtt()
class TestRouter(RouterTestcase):
    broker_class = MQTTBroker
    router_class = MQTTRouter
    route_class = MQTTRoute
    publisher_class = MQTTPublisher

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
                    "url": "mqtt://localhost:1883",
                    "protocol": "mqtt",
                    "protocolVersion": "5.0",
                }
            },
            "channels": {
                "test_test:Handle": {
                    "servers": ["development"],
                    "bindings": {
                        "mqtt": {
                            "topic": "test_test",
                            "qos": 0,
                            "retain": False,
                            "bindingVersion": "0.2.0",
                        }
                    },
                    "publish": {
                        "bindings": {
                            "mqtt": {"qos": 0, "retain": False, "bindingVersion": "0.2.0"}
                        },
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


@pytest.mark.mqtt()
class TestRouterArguments(ArgumentsTestcase):
    broker_class = MQTTRouter

    def get_spec(self, *broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(MQTTBroker(routers=broker))


@pytest.mark.mqtt()
class TestRouterPublisher(PublisherTestcase):
    broker_class = MQTTRouter

    def get_spec(self, *broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(MQTTBroker(routers=broker))
