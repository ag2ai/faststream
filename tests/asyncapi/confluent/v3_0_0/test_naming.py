import pytest
from typing_extensions import override

from faststream.confluent import KafkaBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.confluent()
class TestNaming(NamingTestCase):
    broker_class = KafkaBroker

    # Confluent has no `pattern=`, so every address it can carry is a topic
    # name, and every legal topic name is URI-safe already.
    @pytest.mark.skip(reason="every legal Confluent address is URI-safe")
    @override
    def test_path_channel_refs_are_uri_encoded(self) -> None: ...

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
