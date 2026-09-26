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
    ("url", "expected"),
    (
        pytest.param(
            "redis://user:password@localhost:6379/0",
            "redis://localhost:6379/0",
            id="host keeps its database",
        ),
        pytest.param(
            "redis://user:password@[::1]:6379/0",
            "redis://[::1]:6379/0",
            id="ipv6 keeps its brackets",
        ),
        pytest.param(
            "unix:///tmp/redis.sock?db=0",
            "unix:///tmp/redis.sock?db=0",
            id="socket has no host at all",
        ),
    ),
)
def test_credentials_stripped_from_specification_only(url: str, expected: str) -> None:
    broker = RedisBroker(url)

    assert broker.specification.url == [expected]


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
