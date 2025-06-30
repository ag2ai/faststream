from faststream.redis import RedisBroker, RedisPublisher, RedisRoute, RedisRouter
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v2_6_0.router import RouterTestcase


class TestRouter(RouterTestcase):
    broker_class = RedisBroker
    router_class = RedisRouter
    route_class = RedisRoute
    publisher_class = RedisPublisher

    def test_prefix(self) -> None:
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        @router.subscriber("test")
        async def handle(msg) -> None: ...

        broker.include_router(router)

        schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

        assert schema == {
            "asyncapi": "2.6.0",
            "channels": {
                "test_test:Handle": {
                    "bindings": {
                        "redis": {
                            "bindingVersion": "custom",
                            "channel": "test_test",
                            "method": "subscribe",
                        },
                    },
                    "servers": ["development"],
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
                        "correlationId": {
                            "location": "$message.header#/correlation_id",
                        },
                        "payload": {
                            "$ref": "#/components/schemas/Handle:Message:Payload",
                        },
                        "title": "test_test:Handle:Message",
                    },
                },
                "schemas": {
                    "Handle:Message:Payload": {"title": "Handle:Message:Payload"},
                },
            },
            "defaultContentType": "application/json",
            "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "protocol": "redis",
                    "protocolVersion": "custom",
                    "url": "redis://localhost:6379",
                },
            },
        }


class TestRouterArguments(ArgumentsTestcase):
    broker_class = RedisRouter

    def build_app(self, router):
        broker = RedisBroker()
        broker.include_router(router)
        return broker


class TestRouterPublisher(PublisherTestcase):
    broker_class = RedisRouter

    def build_app(self, router):
        broker = RedisBroker()
        broker.include_router(router)
        return broker
