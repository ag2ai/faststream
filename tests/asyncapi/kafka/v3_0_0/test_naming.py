from typing import Any

import pytest
from typing_extensions import override

from faststream.kafka import KafkaBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.kafka()
class TestNaming(NamingTestCase):
    broker_class = KafkaBroker

    @override
    def uri_unsafe_subscriber(self, broker: Any) -> Any:
        return broker.subscriber(pattern="/test/topic/{user_id}")

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == {
            "asyncapi": "3.0.0",
            "defaultContentType": "application/json",
            "info": {"title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "host": "localhost",
                    "pathname": "",
                    "protocol": "kafka",
                    "protocolVersion": "auto",
                },
            },
            "channels": {
                "test:Handle": {
                    "address": "test:Handle",
                    "servers": [
                        {
                            "$ref": "#/servers/development",
                        },
                    ],
                    "bindings": {"kafka": {"topic": "test", "bindingVersion": "0.4.0"}},
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
                        {
                            "$ref": "#/channels/test:Handle/messages/SubscribeMessage",
                        },
                    ],
                },
            },
            "components": {
                "messages": {
                    "test:Handle:SubscribeMessage": {
                        "title": "test:Handle:SubscribeMessage",
                        "correlationId": {
                            "location": "$message.header#/correlation_id",
                        },
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                    },
                },
                "schemas": {"EmptyPayload": {"title": "EmptyPayload", "type": "null"}},
            },
        }

    def test_pattern(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(pattern="events.*")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert len(schema["channels"]) == 1
        channel = next(iter(schema["channels"].values()))
        assert channel["bindings"]["kafka"]["topic"] == "events.*"

    def test_pattern_template_renders_as_declared(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(pattern="events.{version}")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert len(schema["channels"]) == 1
        channel = next(iter(schema["channels"].values()))
        assert channel["bindings"]["kafka"]["topic"] == "events.{version}"
