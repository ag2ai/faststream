import pytest

from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v2_6_0.naming import NamingTestCase


@pytest.mark.mqtt()
class TestNaming(NamingTestCase):
    broker_class = MQTTBroker

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
                    "url": "mqtt://localhost:1883",
                    "protocol": "mqtt",
                    "protocolVersion": "5.0",
                }
            },
            "channels": {
                "test:Handle": {
                    "servers": ["development"],
                    "bindings": {
                        "mqtt": {
                            "topic": "test",
                            "qos": 0,
                            "retain": False,
                            "bindingVersion": "0.2.0",
                        }
                    },
                    "publish": {
                        "bindings": {
                            "mqtt": {"qos": 0, "retain": False, "bindingVersion": "0.2.0"}
                        },
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
