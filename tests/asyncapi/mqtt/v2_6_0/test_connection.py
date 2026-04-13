import pytest

from faststream.mqtt import MQTTBroker
from faststream.specification import Tag
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.mqtt()
def test_base() -> None:
    broker = MQTTBroker(
        "mqtt:1884",
        version="3.1.1",
        description="Test description",
        tags=(Tag(name="some-tag", description="experimental"),),
    )
    schema = get_2_6_0_schema(broker)

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "description": "Test description",
                "protocol": "mqtt",
                "protocolVersion": "3.1.1",
                "tags": [{"description": "experimental", "name": "some-tag"}],
                "url": "mqtt://mqtt:1884",
            }
        },
    }, schema


@pytest.mark.mqtt()
def test_custom() -> None:
    broker = MQTTBroker(
        "localhost:1884",
        specification_url="mqtt:1885",
    )
    schema = get_2_6_0_schema(broker)

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "mqtt",
                "protocolVersion": "5.0",
                "url": "mqtt:1885",
            }
        },
    }, schema
