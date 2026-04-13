import pytest

from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


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
            "asyncapi": "3.0.0",
            "defaultContentType": "application/json",
            "servers": {
                "development": {
                    "host": "localhost:1883",
                    "pathname": "",
                    "protocol": "mqtt",
                    "protocolVersion": "5.0",
                }
            },
            "channels": {
                "test:Handle": {
                    "address": "test:Handle",
                    "servers": [{"$ref": "#/servers/development"}],
                    "messages": {
                        "SubscribeMessage": {
                            "$ref": "#/components/messages/test:Handle:SubscribeMessage"
                        }
                    },
                    "bindings": {
                        "mqtt": {
                            "topic": "test",
                            "qos": 0,
                            "retain": False,
                            "bindingVersion": "0.2.0",
                        }
                    },
                }
            },
            "operations": {
                "test:HandleSubscribe": {
                    "action": "receive",
                    "channel": {"$ref": "#/channels/test:Handle"},
                    "bindings": {
                        "mqtt": {"qos": 0, "retain": False, "bindingVersion": "0.2.0"}
                    },
                    "messages": [
                        {"$ref": "#/channels/test:Handle/messages/SubscribeMessage"}
                    ],
                }
            },
            "components": {
                "messages": {
                    "test:Handle:SubscribeMessage": {
                        "title": "test:Handle:SubscribeMessage",
                        "correlationId": {"location": "$message.header#/correlation_id"},
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                    }
                },
                "schemas": {"EmptyPayload": {"title": "EmptyPayload", "type": "null"}},
            },
        }
