from faststream.rabbit import (
    RabbitBroker,
    RabbitPublisher,
    RabbitQueue,
    RabbitRoute,
    RabbitRouter,
)
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v2_6_0.router import RouterTestcase


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

        schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

        assert (
            schema
            == {
                "asyncapi": "2.6.0",
                "defaultContentType": "application/json",
                "info": {"title": "FastStream", "version": "0.1.0", "description": ""},
                "servers": {
                    "development": {
                        "url": "amqp://guest:guest@localhost:5672/",  # pragma: allowlist secret
                        "protocol": "amqp",
                        "protocolVersion": "0.9.1",
                    },
                },
                "channels": {
                    "test_test:_:Handle": {
                        "servers": ["development"],
                        "bindings": {
                            "amqp": {
                                "is": "routingKey",
                                "bindingVersion": "0.2.0",
                                "queue": {
                                    "name": "test_test",
                                    "durable": False,
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
                },
                "components": {
                    "messages": {
                        "test_test:_:Handle:Message": {
                            "title": "test_test:_:Handle:Message",
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
        ), schema


class TestRouterArguments(ArgumentsTestcase):
    broker_class = RabbitRouter

    def build_app(self, router):
        broker = RabbitBroker()
        broker.include_router(router)
        return broker


class TestRouterPublisher(PublisherTestcase):
    broker_class = RabbitRouter

    def build_app(self, router):
        broker = RabbitBroker()
        broker.include_router(router)
        return broker
