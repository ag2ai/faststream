import pytest

from faststream.redis import RedisBroker
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.naming import NamingTestCase


class TestNaming(NamingTestCase):
    broker_class = RedisBroker

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = AsyncAPI(broker, schema_version="2.6.0").to_jsonable()

        assert schema == {
            "asyncapi": "2.6.0",
            "channels": {
                "test:Handle": {
                    "bindings": {
                        "redis": {
                            "bindingVersion": "custom",
                            "channel": "test",
                            "method": "subscribe",
                        },
                    },
                    "servers": ["development"],
                    "publish": {
                        "message": {
                            "$ref": "#/components/messages/test:Handle:Message"
                        },
                    },
                },
            },
            "components": {
                "messages": {
                    "test:Handle:Message": {
                        "correlationId": {
                            "location": "$message.header#/correlation_id",
                        },
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                        "title": "test:Handle:Message",
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
                    "url": "redis://localhost:6379",
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
