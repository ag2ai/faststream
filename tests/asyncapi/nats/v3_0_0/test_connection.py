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
    ("servers", "expected_hosts"),
    (
        pytest.param(
            "nats://user:password@localhost:4222",
            ["localhost:4222"],
            id="single-with-credentials",
        ),
        pytest.param(
            ["nats://user:password@host1:4222", "nats://admin:secret@host2:4222"],
            ["host1:4222", "host2:4222"],
            id="multi-with-credentials",
        ),
        pytest.param(
            "nats://mytoken@localhost:4222",
            ["localhost:4222"],
            id="token-auth",
        ),
        pytest.param(
            "nats://user:pass@[::1]:4222",
            ["[::1]:4222"],
            id="ipv6-with-credentials",
        ),
    ),
)
def test_credentials_stripped(
    servers: str | list[str],
    expected_hosts: list[str],
) -> None:
    schema = get_3_0_0_schema(NatsBroker(servers))
    server_values = list(schema["servers"].values())
    actual_hosts = [s["host"] for s in server_values]
    assert actual_hosts == expected_hosts
    for server in server_values:
        assert "@" not in server["host"]


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
