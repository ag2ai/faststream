from faststream.redis import RedisBroker
from faststream.specification import Tag
from faststream.specification.asyncapi import AsyncAPI


def test_base() -> None:
    schema = AsyncAPI(
        RedisBroker(
            "redis://localhost:6379",
            protocol="plaintext",
            protocol_version="0.9.0",
            description="Test description",
            tags=(Tag(name="some-tag", description="experimental"),),
        ),
        schema_version="2.6.0",
    ).to_jsonable()

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "description": "Test description",
                "protocol": "plaintext",
                "protocolVersion": "0.9.0",
                "tags": [{"description": "experimental", "name": "some-tag"}],
                "url": "redis://localhost:6379",
            },
        },
    }, schema


def test_custom() -> None:
    schema = AsyncAPI(
        RedisBroker(
            "redis://localhost:6379",
            specification_url="rediss://127.0.0.1:8000",
        ),
        schema_version="2.6.0",
    ).to_jsonable()

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "rediss",
                "protocolVersion": "custom",
                "url": "rediss://127.0.0.1:8000",
            },
        },
    }
