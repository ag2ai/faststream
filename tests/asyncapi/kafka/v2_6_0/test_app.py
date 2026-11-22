import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.kafka import KafkaBroker
from faststream.specification import Contact, ExternalDocs, License, Tag
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.kafka()
def test_base(snapshot_json: SnapshotAssertion) -> None:
    schema = get_2_6_0_schema(KafkaBroker())
    assert schema == snapshot_json


@pytest.mark.kafka()
def test_with_name(snapshot_json: SnapshotAssertion) -> None:
    schema = get_2_6_0_schema(
        KafkaBroker(),
        title="My App",
        version="1.0.0",
        description="Test description",
    )

    assert schema == snapshot_json


@pytest.mark.kafka()
def test_full(snapshot_json: SnapshotAssertion) -> None:
    schema = get_2_6_0_schema(
        KafkaBroker(),
        title="My App",
        version="1.0.0",
        description="Test description",
        license=License(name="MIT", url="https://mit.com/"),
        terms_of_service="https://my-terms.com/",
        contact=Contact(name="support", url="https://help.com/"),
        tags=(Tag(name="some-tag", description="experimental"),),
        identifier="some-unique-uuid",
        external_docs=ExternalDocs(
            url="https://extra-docs.py/",
        ),
    )

    assert schema == snapshot_json


@pytest.mark.kafka()
def test_full_dict(snapshot_json: SnapshotAssertion) -> None:
    schema = get_2_6_0_schema(
        KafkaBroker(),
        title="My App",
        version="1.0.0",
        description="Test description",
        license={"name": "MIT", "url": "https://mit.com/"},
        terms_of_service="https://my-terms.com/",
        contact={"name": "support", "url": "https://help.com/"},
        tags=({"name": "some-tag", "description": "experimental"},),
        identifier="some-unique-uuid",
        external_docs={
            "url": "https://extra-docs.py/",
        },
    )

    assert schema == snapshot_json


@pytest.mark.kafka()
def test_extra(snapshot_json: SnapshotAssertion) -> None:
    schema = get_2_6_0_schema(
        KafkaBroker(),
        title="My App",
        version="1.0.0",
        description="Test description",
        license={"name": "MIT", "url": "https://mit.com/", "x-field": "extra"},
        terms_of_service="https://my-terms.com/",
        contact={"name": "support", "url": "https://help.com/", "x-field": "extra"},
        tags=({"name": "some-tag", "description": "experimental", "x-field": "extra"},),
        identifier="some-unique-uuid",
        external_docs={
            "url": "https://extra-docs.py/",
            "x-field": "extra",
        },
    )

    assert schema == snapshot_json
