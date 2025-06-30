from faststream.confluent import KafkaBroker
from faststream.specification import Tag
from faststream.specification.asyncapi import AsyncAPI


def test_base() -> None:
    schema = AsyncAPI(
        KafkaBroker(
            "kafka:9092",
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
                "url": "kafka:9092",
            },
        },
    }


def test_multi() -> None:
    schema = AsyncAPI(
        KafkaBroker(["kafka:9092", "kafka:9093"]),
        schema_version="2.6.0",
    ).to_jsonable()

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
        "servers": {
            "Server1": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "url": "kafka:9092",
            },
            "Server2": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "url": "kafka:9093",
            },
        },
    }


def test_custom() -> None:
    schema = AsyncAPI(
        KafkaBroker(
            ["kafka:9092", "kafka:9093"],
            specification_url=["kafka:9094", "kafka:9095"],
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
            "Server1": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "url": "kafka:9094",
            },
            "Server2": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "url": "kafka:9095",
            },
        },
    }
