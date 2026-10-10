import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.nats import NatsBroker
from faststream.specification import Tag
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.nats()
def test_base(snapshot_json: SnapshotAssertion) -> None:
    broker = NatsBroker(
        "nats:9092",
        protocol="plaintext",
        protocol_version="0.9.0",
        description="Test description",
        tags=(Tag(name="some-tag", description="experimental"),),
    )
    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json


@pytest.mark.nats()
def test_multi(snapshot_json: SnapshotAssertion) -> None:
    broker = NatsBroker(["nats:9092", "nats:9093"])
    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json


@pytest.mark.nats()
def test_custom(snapshot_json: SnapshotAssertion) -> None:
    broker = NatsBroker(
        ["nats:9092", "nats:9093"],
        specification_url=["nats:9094", "nats:9095"],
    )
    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json
