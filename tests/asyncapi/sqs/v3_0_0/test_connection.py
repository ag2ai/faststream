import pytest

from faststream.specification import Tag
from faststream.sqs import SQSBroker
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.sqs()
def test_base() -> None:
    broker = SQSBroker(
        endpoint_url="http://localhost:4566",
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
                "host": "localhost:4566",
                "pathname": "",
                "protocol": "sqs",
                "description": "Test description",
                "protocolVersion": "custom",
                "tags": [{"name": "some-tag", "description": "experimental"}],
            }
        },
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
    }, schema


@pytest.mark.sqs()
def test_custom() -> None:
    broker = SQSBroker(specification_url="custom-sqs-url")
    schema = get_3_0_0_schema(broker)

    assert schema == {
        "info": {"title": "FastStream", "version": "0.1.0"},
        "asyncapi": "3.0.0",
        "defaultContentType": "application/json",
        "servers": {
            "development": {
                "host": "custom-sqs-url",
                "pathname": "",
                "protocol": "sqs",
                "protocolVersion": "custom",
            }
        },
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
    }
