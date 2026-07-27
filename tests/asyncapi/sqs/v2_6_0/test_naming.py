import pytest

from faststream.sqs import SQSBroker
from tests.asyncapi.base.v2_6_0.naming import NamingTestCase


@pytest.mark.sqs()
class TestNaming(NamingTestCase):
    broker_class = SQSBroker

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == {
            "info": {"title": "FastStream", "version": "0.1.0"},
            "asyncapi": "2.6.0",
            "defaultContentType": "application/json",
            "servers": {
                "development": {
                    "url": "sqs",
                    "protocol": "sqs",
                    "protocolVersion": "custom",
                }
            },
            "channels": {
                "test:Handle": {
                    "servers": ["development"],
                    "bindings": {
                        "sqs": {
                            "queue": {"name": "test", "fifo": False},
                            "bindingVersion": "custom",
                        }
                    },
                    "publish": {
                        "bindings": {"sqs": {"bindingVersion": "custom"}},
                        "message": {"$ref": "#/components/messages/test:Handle:Message"},
                    },
                }
            },
            "components": {
                "messages": {
                    "test:Handle:Message": {
                        "title": "test:Handle:Message",
                        "correlationId": {"location": "$message.header#/correlation_id"},
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                    }
                },
                "schemas": {"EmptyPayload": {"title": "EmptyPayload", "type": "null"}},
            },
        }
