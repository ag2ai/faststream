import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.redis import RedisBroker
from faststream.specification import Tag
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.redis()
def test_base(snapshot_json: SnapshotAssertion) -> None:
    schema = get_2_6_0_schema(
        RedisBroker(
            "redis://localhost:6379",
            protocol="plaintext",
            protocol_version="0.9.0",
            description="Test description",
            tags=(Tag(name="some-tag", description="experimental"),),
        ),
    )

    assert schema == snapshot_json


@pytest.mark.redis()
def test_custom(snapshot_json: SnapshotAssertion) -> None:
    schema = get_2_6_0_schema(
        RedisBroker(
            "redis://localhost:6379",
            specification_url="rediss://127.0.0.1:8000",
        ),
    )

    assert schema == snapshot_json
