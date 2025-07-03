from faststream.confluent import KafkaBroker
from faststream.specification import Tag
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema


def test_base() -> None:
    schema = get_2_6_0_schema(
        KafkaBroker(
            "kafka:9092",
            protocol="plaintext",
            protocol_version="0.9.0",
            description="Test description",
            tags=(Tag(name="some-tag", description="experimental"),),
        )
    )
    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
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
    schema = get_2_6_0_schema(KafkaBroker(["kafka:9092", "kafka:9093"]))

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
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
    schema = get_2_6_0_schema(
        KafkaBroker(
            ["kafka:9092", "kafka:9093"],
            specification_url=["kafka:9094", "kafka:9095"],
        )
    )

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
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
