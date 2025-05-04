import pytest

from faststream.redis import RedisBroker
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


class TestNaming(NamingTestCase):
    broker_class = RedisBroker

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = AsyncAPI(
            broker,
            schema_version="3.0.0",
        ).to_jsonable()

        assert schema == {
            "asyncapi": "3.0.0",
            "channels": {
                "test:Handle": {
                    "address": "test:Handle",
                    "bindings": {
                        "redis": {
                            "bindingVersion": "custom",
                            "channel": "test",
                            "method": "subscribe",
                        },
                    },
                    "servers": [{"$ref": "#/servers/development"}],
                    "messages": {
                        "SubscribeMessage": {
                            "$ref": "#/components/messages/test:Handle:SubscribeMessage",
                        },
                    },
                },
            },
            "operations": {
                "test:HandleSubscribe": {
                    "action": "receive",
                    "channel": {
                        "$ref": "#/channels/test:Handle",
                    },
                    "messages": [
                        {"$ref": "#/channels/test:Handle/messages/SubscribeMessage"},
                    ],
                },
            },
            "components": {
                "messages": {
                    "test:Handle:SubscribeMessage": {
                        "correlationId": {
                            "location": "$message.header#/correlation_id",
                        },
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                        "title": "test:Handle:SubscribeMessage",
                    },
                },
                "schemas": {"EmptyPayload": {"title": "EmptyPayload", "type": "null"}},
            },
            "defaultContentType": "application/json",
            "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "protocol": "redis",
                    "protocolVersion": "custom",
                    "host": "localhost:6379",
                    "pathname": "",
                },
            },
        }, schema

    @pytest.mark.parametrize(
        "args",
        (
            pytest.param({"channel": "test"}, id="channel"),
            pytest.param({"list": "test"}, id="list"),
            pytest.param({"stream": "test"}, id="stream"),
        ),
    )
    def test_subscribers_variations(self, args) -> None:
        broker = self.broker_class()

        @broker.subscriber(**args)
        async def handle() -> None: ...

        schema = AsyncAPI(broker)
        assert list(schema.to_jsonable()["channels"].keys()) == ["test:Handle"]

    @pytest.mark.parametrize(
        "args",
        (
            pytest.param({"channel": "test"}, id="channel"),
            pytest.param({"list": "test"}, id="list"),
            pytest.param({"stream": "test"}, id="stream"),
        ),
    )
    def test_publisher_variations(self, args) -> None:
        broker = self.broker_class()

        @broker.publisher(**args)
        async def handle() -> None: ...

        schema = AsyncAPI(broker)
        assert list(schema.to_jsonable()["channels"].keys()) == ["test:Publisher"]
