import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.mqtt import MQTTBroker
from faststream.specification import Tag
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.mqtt()
def test_base(snapshot_json: SnapshotAssertion) -> None:
    broker = MQTTBroker(
        "mqtt:1884",
        version="3.1.1",
        description="Test description",
        tags=(Tag(name="some-tag", description="experimental"),),
    )
    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json


@pytest.mark.mqtt()
def test_custom(snapshot_json: SnapshotAssertion) -> None:
    broker = MQTTBroker(
        "localhost:1884",
        specification_url="mqtt:1885",
    )
    schema = get_2_6_0_schema(broker)

    assert schema == snapshot_json


@pytest.mark.mqtt()
def test_tls_url(snapshot_json: SnapshotAssertion) -> None:
    schema = get_2_6_0_schema(MQTTBroker("mqtts://user:password@localhost"))

    assert schema == snapshot_json
    assert "password" not in repr(schema)
