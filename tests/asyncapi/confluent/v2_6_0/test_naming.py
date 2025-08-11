import pytest

from faststream.confluent import KafkaBroker
from tests.asyncapi.base.v2_6_0.naming import NamingTestCase


@pytest.mark.confluent()
class TestNaming(NamingTestCase):
    broker_class = KafkaBroker

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == {
            "asyncapi": "2.6.0",
            "defaultContentType": "application/json",
            "info": {"title": "FastStream", "version": "0.1.0"},
            "servers": {
                "development": {
                    "url": "localhost",
                    "protocol": "kafka",
                    "protocolVersion": "auto",
                },
            },
            "channels": {
                "test:Handle": {
                    "servers": ["development"],
                    "bindings": {"kafka": {"topic": "test", "bindingVersion": "0.4.0"}},
                    "publish": {
                        "message": {
                            "$ref": "#/components/messages/test:Handle:Message",
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
