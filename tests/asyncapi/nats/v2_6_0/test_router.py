from faststream.nats import NatsBroker, NatsPublisher, NatsRoute, NatsRouter
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v2_6_0.router import RouterTestcase


class TestRouter(RouterTestcase):
    broker_class = NatsBroker
    router_class = NatsRouter
    route_class = NatsRoute
    publisher_class = NatsPublisher

    def test_prefix(self) -> None:
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        @router.subscriber("test")
        async def handle(msg) -> None: ...

        broker.include_router(router)

        schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

        assert schema == {
            "asyncapi": "2.6.0",
            "defaultContentType": "application/json",
            "info": {"title": "FastStream", "version": "0.1.0", "description": ""},
            "servers": {
                "development": {
                    "url": "nats://localhost:4222",
                    "protocol": "nats",
                    "protocolVersion": "custom",
                },
            },
            "channels": {
                "test_test:Handle": {
                    "servers": ["development"],
                    "bindings": {
                        "nats": {"subject": "test_test", "bindingVersion": "custom"},
                    },
                    "publish": {
                        "message": {
                            "$ref": "#/components/messages/test_test:Handle:Message",
                        },
                    },
                },
            },
            "components": {
                "messages": {
                    "test_test:Handle:Message": {
                        "title": "test_test:Handle:Message",
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


class TestRouterArguments(ArgumentsTestcase):
    broker_class = NatsRouter

    def build_app(self, router):
        broker = NatsBroker()
        broker.include_router(router)
        return broker


class TestRouterPublisher(PublisherTestcase):
    broker_class = NatsRouter

    def build_app(self, router):
        broker = NatsBroker()
        broker.include_router(router)
        return broker
