import pytest

from faststream.redis import RedisBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.redis()
class TestNaming(NamingTestCase):
    broker_class = RedisBroker

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

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
                "test:Handle": {
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
            "info": {"title": "FastStream", "version": "0.1.0"},
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
    def test_subscribers_variations(self, args: dict[str, str]) -> None:
        broker = self.broker_class()

        @broker.subscriber(**args)
        async def handle() -> None: ...

        schema = self.get_spec(broker)
        assert list(schema.to_jsonable()["channels"].keys()) == ["test:Handle"]

    @pytest.mark.parametrize(
        "args",
        (
            pytest.param({"channel": "test"}, id="channel"),
            pytest.param({"list": "test"}, id="list"),
            pytest.param({"stream": "test"}, id="stream"),
        ),
    )
    def test_publisher_variations(self, args: dict[str, str]) -> None:
        broker = self.broker_class()

        @broker.publisher(**args)
        async def handle() -> None: ...

        schema = self.get_spec(broker)
        assert list(schema.to_jsonable()["channels"].keys()) == ["test:Publisher"]
