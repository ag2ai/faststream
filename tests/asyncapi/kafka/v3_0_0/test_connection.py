from faststream.kafka import KafkaBroker
from faststream.specification import Tag
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema


def test_base() -> None:
    schema = get_3_0_0_schema(
        KafkaBroker(
            "kafka:9092",
            protocol="plaintext",
            protocol_version="0.9.0",
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
                "protocol": "plaintext",
                "protocolVersion": "0.9.0",
                "tags": [{"description": "experimental", "name": "some-tag"}],
                "host": "kafka:9092",
                "pathname": "",
            },
        },
    }


def test_multi() -> None:
    schema = get_3_0_0_schema(KafkaBroker(["kafka:9092", "kafka:9093"]))

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "Server1": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "host": "kafka:9092",
                "pathname": "",
            },
            "Server2": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "host": "kafka:9093",
                "pathname": "",
            },
        },
    }


def test_custom() -> None:
    schema = get_3_0_0_schema(
        KafkaBroker(
            ["kafka:9092", "kafka:9093"],
            specification_url=["kafka:9094", "kafka:9095"],
        )
    )

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "Server1": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "host": "kafka:9094",
                "pathname": "",
            },
            "Server2": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "host": "kafka:9095",
                "pathname": "",
            },
        },
    }
