from faststream.nats import NatsBroker
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.naming import NamingTestCase


class TestNaming(NamingTestCase):
    broker_class = NatsBroker

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

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
                "test:Handle": {
                    "servers": ["development"],
                    "bindings": {
                        "nats": {"subject": "test", "bindingVersion": "custom"},
                    },
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
                        "title": "test:Handle:Message",
                        "correlationId": {
                            "location": "$message.header#/correlation_id",
                        },
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                    },
                },
                "schemas": {"EmptyPayload": {"title": "EmptyPayload", "type": "null"}},
            },
        }
