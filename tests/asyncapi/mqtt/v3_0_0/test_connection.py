import pytest

from faststream.mqtt import MQTTBroker
from faststream.specification import Tag
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.mqtt()
def test_base() -> None:
    broker = MQTTBroker(
        "mqtt:1884",
        version="3.1.1",
        description="Test description",
        tags=(Tag(name="some-tag", description="experimental"),),
    )
    schema = get_3_0_0_schema(broker)

    assert schema == {
        "info": {"title": "FastStream", "version": "0.1.0"},
        "asyncapi": "3.0.0",
        "defaultContentType": "application/json",
        "servers": {
            "development": {
                "host": "mqtt:1884",
                "pathname": "",
                "protocol": "mqtt",
                "description": "Test description",
                "protocolVersion": "3.1.1",
                "tags": [{"name": "some-tag", "description": "experimental"}],
            }
        },
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
    }, schema


@pytest.mark.mqtt()
def test_custom() -> None:
    broker = MQTTBroker(
        "localhost:1884",
        specification_url="mqtt:1885",
    )
    schema = get_3_0_0_schema(broker)

    assert schema == {
        "info": {"title": "FastStream", "version": "0.1.0"},
        "asyncapi": "3.0.0",
        "defaultContentType": "application/json",
        "servers": {
            "development": {
                "host": "mqtt:1885",
                "pathname": "",
                "protocol": "mqtt",
                "protocolVersion": "5.0",
            }
        },
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
    }
