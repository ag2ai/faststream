from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.specification import AsyncAPI, Tag


def test_base() -> None:
    broker = NatsBroker(
        "nats:9092",
        protocol="plaintext",
        protocol_version="0.9.0",
        description="Test description",
        tags=(Tag(name="some-tag", description="experimental"),),
    )
    schema = (
        FastStream(broker, specification=AsyncAPI(schema_version="2.6.0"))
        .schema.to_specification()
        .to_jsonable()
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
                "url": "nats:9092",
            },
        },
    }, schema


def test_multi() -> None:
    broker = NatsBroker(["nats:9092", "nats:9093"])
    schema = (
        FastStream(broker, specification=AsyncAPI(schema_version="2.6.0"))
        .schema.to_specification()
        .to_jsonable()
    )

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "Server1": {
                "protocol": "nats",
                "protocolVersion": "custom",
                "url": "nats:9092",
            },
            "Server2": {
                "protocol": "nats",
                "protocolVersion": "custom",
                "url": "nats:9093",
            },
        },
    }


def test_custom() -> None:
    broker = NatsBroker(
        ["nats:9092", "nats:9093"],
        specification_url=["nats:9094", "nats:9095"],
    )

    schema = (
        FastStream(broker, specification=AsyncAPI(schema_version="2.6.0"))
        .schema.to_specification()
        .to_jsonable()
    )

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "Server1": {
                "protocol": "nats",
                "protocolVersion": "custom",
                "url": "nats:9094",
            },
            "Server2": {
                "protocol": "nats",
                "protocolVersion": "custom",
                "url": "nats:9095",
            },
        },
    }
