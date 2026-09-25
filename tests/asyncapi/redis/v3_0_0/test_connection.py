import pytest

from faststream.redis import RedisBroker
from faststream.specification import Tag
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.redis()
def test_base() -> None:
    schema = get_3_0_0_schema(
        RedisBroker(
            "redis://localhost:6379",
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
                "host": "localhost:6379",
                "pathname": "",
            },
        },
    }, schema


@pytest.mark.redis()
@pytest.mark.parametrize(
    ("url", "expected_host"),
    (
        pytest.param(
            "redis://user:password@localhost:6379/0",
            "localhost:6379",
            id="with-credentials",
        ),
        pytest.param(
            "rediss://user:password@host:6380/0",
            "host:6380",
            id="rediss-with-credentials",
        ),
        pytest.param(
            "redis://user:password@[::1]:6379/0",
            "[::1]:6379",
            id="ipv6-with-credentials",
        ),
    ),
)
def test_credentials_stripped(url: str, expected_host: str) -> None:
    schema = get_3_0_0_schema(RedisBroker(url))
    server = schema["servers"]["development"]
    assert server["host"] == expected_host
    assert "@" not in server["host"]


@pytest.mark.redis()
def test_custom() -> None:
    schema = get_3_0_0_schema(
        RedisBroker(
            "redis://localhost:6379",
            specification_url="rediss://127.0.0.1:8000",
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
                "protocol": "rediss",
                "protocolVersion": "custom",
                "host": "127.0.0.1:8000",
                "pathname": "",
            },
        },
    }
