import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.confluent import KafkaBroker
from faststream.specification import Tag
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.confluent()
def test_base(snapshot_json: SnapshotAssertion) -> None:
    schema = get_3_0_0_schema(
        KafkaBroker(
            "kafka:9092",
            protocol="plaintext",
            protocol_version="0.9.0",
            description="Test description",
            tags=(Tag(name="some-tag", description="experimental"),),
        ),
    )

    assert schema == snapshot_json


@pytest.mark.confluent()
def test_multi(snapshot_json: SnapshotAssertion) -> None:
    schema = get_3_0_0_schema(
        KafkaBroker(["kafka:9092", "kafka:9093"]),
    )

    assert schema == snapshot_json


@pytest.mark.confluent()
def test_custom(snapshot_json: SnapshotAssertion) -> None:
    schema = get_3_0_0_schema(
        KafkaBroker(
            ["kafka:9092", "kafka:9093"],
            specification_url=["kafka:9094", "kafka:9095"],
        ),
    )

    assert schema == snapshot_json
