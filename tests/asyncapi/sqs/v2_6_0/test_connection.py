import pytest

from faststream.specification import Tag
from faststream.sqs import SQSBroker
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.sqs()
def test_base() -> None:
    broker = SQSBroker(
        endpoint_url="http://localhost:4566",
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
                "protocol": "sqs",
                "protocolVersion": "custom",
                "tags": [{"description": "experimental", "name": "some-tag"}],
                "url": "http://localhost:4566",
            }
        },
    }, schema


@pytest.mark.sqs()
def test_custom() -> None:
    broker = SQSBroker(specification_url="custom-sqs-url")
    schema = get_2_6_0_schema(broker)

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "sqs",
                "protocolVersion": "custom",
                "url": "custom-sqs-url",
            }
        },
    }, schema
