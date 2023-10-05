from faststream import FastStream
from faststream.asyncapi.generate import get_app_schema
from faststream.nats import NatsBroker, NatsRoute, NatsRouter
from tests.asyncapi.base.arguments import ArgumentsTestcase
from tests.asyncapi.base.publisher import PublisherTestcase
from tests.asyncapi.base.router import RouterTestcase


class TestRouter(RouterTestcase):
    broker_class = NatsBroker
    router_class = NatsRouter
    route_class = NatsRoute

    def test_prefix(self):
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        @router.subscriber("test")
        async def handle(msg):
            ...

        broker.include_router(router)

        schema = get_app_schema(FastStream(broker)).to_jsonable()

        print(schema)

        assert schema == {
            "asyncapi": "2.6.0",
            "defaultContentType": "application/json",
            "info": {"title": "FastStream", "version": "0.1.0", "description": ""},
            "servers": {
                "development": {
                    "url": "nats://localhost:4222",
                    "protocol": "nats",
                    "protocolVersion": "custom",
                }
            },
            "channels": {
                "test_test:Handle": {
                    "servers": ["development"],
                    "bindings": {
                        "nats": {"subject": "test_test", "bindingVersion": "custom"}
                    },
                    "subscribe": {
                        "message": {
                            "$ref": "#/components/messages/test_test:Handle:Message"
                        }
                    },
                }
            },
            "components": {
                "messages": {
                    "test_test:Handle:Message": {
                        "title": "test_test:Handle:Message",
                        "correlationId": {
                            "location": "$message.header#/correlation_id"
                        },
                        "payload": {
                            "$ref": "#/components/schemas/test_test:Handle:Message:Msg:Payload"
                        },
                    }
                },
                "schemas": {
                    "test_test:Handle:Message:Msg:Payload": {
                        "title": "test_test:Handle:Message:Msg:Payload"
                    }
                },
            },
        }


class TestRouterArguments(ArgumentsTestcase):
    broker_class = NatsRouter

    def build_app(self, router):
        broker = NatsBroker()
        broker.include_router(router)
        return FastStream(broker)


class TestRouterPublisher(PublisherTestcase):
    broker_class = NatsRouter

    def build_app(self, router):
        broker = NatsBroker()
        broker.include_router(router)
        return FastStream(broker)
