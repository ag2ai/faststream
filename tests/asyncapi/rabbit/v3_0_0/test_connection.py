import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.rabbit import RabbitBroker
from faststream.specification import Tag
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.rabbit()
def test_base(snapshot_json: SnapshotAssertion) -> None:
    schema = get_3_0_0_schema(
        RabbitBroker(
            "amqps://localhost",
            port=5673,
            protocol_version="0.9.0",
            description="Test description",
            tags=(Tag(name="some-tag", description="experimental"),),
        ),
    )

    assert schema == snapshot_json


@pytest.mark.rabbit()
def test_kwargs() -> None:
    broker = RabbitBroker(
        "amqp://guest:guest@localhost:5672/?heartbeat=300",
        host="127.0.0.1",
    )

    assert broker.specification.url == [
        "amqp://guest:guest@127.0.0.1:5672/?heartbeat=300",
    ]


@pytest.mark.rabbit()
def test_custom(snapshot_json: SnapshotAssertion) -> None:
    broker = RabbitBroker(
        "amqps://localhost",
        specification_url="amqp://guest:guest@127.0.0.1:5672/vh",
    )

    pub = broker.publisher("test")  # noqa: F841
    schema = get_3_0_0_schema(broker)

    assert schema == snapshot_json
