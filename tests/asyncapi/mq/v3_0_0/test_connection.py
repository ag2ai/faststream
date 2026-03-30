import pytest

from faststream.mq import MQBroker
from faststream.specification import Tag
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.mq()
def test_base() -> None:
    schema = get_3_0_0_schema(
        MQBroker(
            "QM1",
            conn_name="mq.example.com(1414)",
            protocol="mqi",
            protocol_version="9.4",
            description="Test description",
            tags=(Tag(name="some-tag", description="experimental"),),
        ),
    )

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "description": "Test description",
                "protocol": "mqi",
                "protocolVersion": "9.4",
                "tags": [{"description": "experimental", "name": "some-tag"}],
                "host": "QM1@mq.example.com(1414)",
                "pathname": "",
            },
        },
    }


@pytest.mark.mq()
def test_custom() -> None:
    schema = get_3_0_0_schema(
        MQBroker(
            "QM1",
            conn_name="mq.example.com(1414)",
            specification_url="mq://DEV@mq.internal(1515)",
        ),
    )

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "ibmmq",
                "protocolVersion": "mqi",
                "host": "DEV@mq.internal(1515)",
                "pathname": "",
            },
        },
    }
