import pytest

from faststream.nats import NatsBroker
from faststream.specification import Tag
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.nats()
def test_base() -> None:
    broker = NatsBroker(
        "nats:9092",
        protocol="plaintext",
        protocol_version="0.9.0",
        description="Test description",
        tags=(Tag(name="some-tag", description="experimental"),),
    )
    schema = get_3_0_0_schema(broker)

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
                "host": "nats:9092",
                "pathname": "",
            },
        },
    }, schema


@pytest.mark.nats()
@pytest.mark.parametrize(
    ("servers", "expected"),
    (
        pytest.param(
            ["nats://user:password@host1:4222", "admin:secret@host2:4222"],
            ["nats://host1:4222", "host2:4222"],
            id="schemed and scheme-less",
        ),
        pytest.param(
            ["nats://user:password@[::1]:4222"],
            ["nats://[::1]:4222"],
            id="ipv6 keeps its brackets",
        ),
        pytest.param(
            ["nats://mytoken@localhost:4222"],
            ["nats://localhost:4222"],
            id="token without a password",
        ),
    ),
)
def test_credentials_stripped(
    servers: list[str],
    expected: list[str],
) -> None:
    broker = NatsBroker(servers)

    assert broker.specification.url == expected


@pytest.mark.nats()
def test_multi() -> None:
    broker = NatsBroker(["nats:9092", "nats:9093"])
    schema = get_3_0_0_schema(broker)

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "Server1": {
                "protocol": "nats",
                "protocolVersion": "custom",
                "host": "nats:9092",
                "pathname": "",
            },
            "Server2": {
                "protocol": "nats",
                "protocolVersion": "custom",
                "host": "nats:9093",
                "pathname": "",
            },
        },
    }


@pytest.mark.nats()
def test_custom() -> None:
    broker = NatsBroker(
        ["nats:9092", "nats:9093"],
        specification_url=["nats:9094", "nats:9095"],
    )
    schema = get_3_0_0_schema(broker)

    assert schema == {
        "asyncapi": "3.0.0",
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "Server1": {
                "protocol": "nats",
                "protocolVersion": "custom",
                "host": "nats:9094",
                "pathname": "",
            },
            "Server2": {
                "protocol": "nats",
                "protocolVersion": "custom",
                "host": "nats:9095",
                "pathname": "",
            },
        },
    }
